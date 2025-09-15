package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

/**
todo:
0. Сделать обертку над Business +
1. Ограничение прав доступа к вызываем функциями через ACL
(через мидлвару с проверкой метаданных к ACL) +
2. Метод логирования +
3. Метод статистики
*/

type Acl map[string][]string

type BizService struct {
	UnimplementedBizServer
}

func NewBizService() BizService {
	return BizService{}
}

func (s BizService) Check(ctx context.Context, n *Nothing) (*Nothing, error) {
	return n, nil
}

func (s BizService) Add(ctx context.Context, n *Nothing) (*Nothing, error) {
	return n, nil
}

func (s BizService) Test(ctx context.Context, n *Nothing) (*Nothing, error) {
	return n, nil
}

func getConsumer(ctx context.Context) (string, error) {
	md, _ := metadata.FromIncomingContext(ctx)

	consumer := md.Get("consumer")
	if len(consumer) <= 0 {
		return "", errors.New("ctx doesnt have consumer")
	}

	return consumer[0], nil
}

type Logger struct {
	connections map[string]chan *Event
	mu          sync.Mutex
}

func NewLogger() *Logger {
	return &Logger{
		connections: make(map[string]chan *Event),
	}
}

func (l *Logger) HasConnections() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	conns := len(l.connections)
	return conns > 0
}

func (l *Logger) Send(e *Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, ch := range l.connections {
		ch <- e
	}
}

func (l *Logger) GetCh(consumer string) <-chan *Event {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.connections[consumer]
}

func (l *Logger) AddConnection(consumer string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	ch := make(chan *Event)

	_, has := l.connections[consumer]
	if has {
		fmt.Printf("consumer=%s already has conenction to logger", consumer)
		return
	}

	l.connections[consumer] = ch
}

func (l *Logger) RemoveConnection(consumer string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	conn, ok := l.connections[consumer]
	if !ok {
		return
	}

	close(conn)
	delete(l.connections, consumer)
}

type AdminService struct {
	Logger *Logger
	Host   string
	UnimplementedAdminServer
}

func NewAdminService(host string) AdminService {
	return AdminService{
		Logger: NewLogger(),
		Host:   host,
	}
}

func (s AdminService) Logging(params *Nothing, srv Admin_LoggingServer) error {
	ctx := srv.Context()
	consumer, err := getConsumer(ctx)
	if err != nil {
		return err
	}

	s.Logger.AddConnection(consumer)
	ch := s.Logger.GetCh(consumer)

	for event := range ch {
		err := srv.Send(event)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func (s AdminService) Statistics(params *StatInterval, srv Admin_StatisticsServer) error {
	return nil
}

func checkAllowedMethods(allowedMethods []string, method string) bool {
	methodSplitted := strings.Split(method, "/")
	for _, allowedMethod := range allowedMethods {
		splittedAllowedMethod := strings.Split(allowedMethod, "/")
		allowed := true
		for j, allowedMethodPart := range splittedAllowedMethod {
			if methodSplitted[j] == allowedMethodPart {
				continue
			}

			if allowedMethodPart == "*" {
				return true
			}

			allowed = false
			break
		}

		if allowed {
			return true
		}
	}

	return false
}

func checkAuth(ctx context.Context, acl Acl, fullMethod string) error {
	authErr := status.Error(codes.Unauthenticated, "Unauthenticated")

	consumer, err := getConsumer(ctx)
	if err != nil {
		return authErr
	}

	methods, ok := acl[consumer]
	if !ok {
		return authErr
	}

	if !checkAllowedMethods(methods, fullMethod) {
		return authErr
	}

	return nil
}

func authInterceptor(acl Acl) func(
	context.Context,
	interface{},
	*grpc.UnaryServerInfo,
	grpc.UnaryHandler,
) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		err := checkAuth(ctx, acl, info.FullMethod)
		if err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

func authInterceptorStream(acl Acl) func(interface{}, grpc.ServerStream, *grpc.StreamServerInfo, grpc.StreamHandler) error {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()
		err := checkAuth(ctx, acl, info.FullMethod)
		if err != nil {
			return err
		}

		return handler(srv, ss)
	}
}

func (s *AdminService) createEvent(consumer string, method string) *Event {
	host := s.Host + method

	return &Event{
		Consumer: consumer,
		Method:   method,
		Host:     host,
	}
}

func logInterceptor(s AdminService) func(
	context.Context,
	interface{},
	*grpc.UnaryServerInfo,
	grpc.UnaryHandler,
) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if !s.Logger.HasConnections() {
			return handler(ctx, req)
		}

		method := info.FullMethod
		consumer, _ := getConsumer(ctx)

		event := s.createEvent(consumer, method)
		s.Logger.Send(event)

		return handler(ctx, req)
	}
}

func logInterceptorStream(s AdminService) func(interface{}, grpc.ServerStream, *grpc.StreamServerInfo, grpc.StreamHandler) error {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()

		if !s.Logger.HasConnections() {
			return handler(srv, ss)
		}

		method := info.FullMethod
		consumer, _ := getConsumer(ctx)

		event := s.createEvent(consumer, method)
		s.Logger.Send(event)

		return handler(srv, ss)
	}
}

func StartMyMicroservice(ctx context.Context, addr string, aclData string) error {
	acl := make(Acl)
	err := json.Unmarshal([]byte(aclData), &acl)
	if err != nil {
		return err
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	admService := NewAdminService(addr)
	bizService := NewBizService()

	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(authInterceptor(acl), logInterceptor(admService)),
		grpc.ChainStreamInterceptor(authInterceptorStream(acl), logInterceptorStream(admService)),
	)

	RegisterAdminServer(server, admService)
	RegisterBizServer(server, bizService)

	go func(ctx context.Context) {
		server.Serve(lis)
	}(ctx)

	go func(ctx context.Context) {
		<-ctx.Done()
		server.GracefulStop()
	}(ctx)

	return err
}
