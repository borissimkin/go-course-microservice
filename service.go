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
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

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

type StatisticConnection struct {
	Stat *Stat
}

func NewStatConenction() *StatisticConnection {
	return &StatisticConnection{
		Stat: &Stat{
			ByMethod:   make(map[string]uint64),
			ByConsumer: make(map[string]uint64),
		},
	}
}

func (s *StatisticConnection) Clear() {
	s.Stat = &Stat{
		ByMethod:   make(map[string]uint64),
		ByConsumer: make(map[string]uint64),
	}
}

type StatisticStreamer struct {
	mu          sync.Mutex
	connections map[string]*StatisticConnection
}

func NewStatStreamer() *StatisticStreamer {
	return &StatisticStreamer{
		connections: make(map[string]*StatisticConnection),
	}
}

func (s *StatisticStreamer) HasConnections() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	conns := len(s.connections)
	return conns > 0
}

func (s *StatisticStreamer) GetConnection(consumer string) *StatisticConnection {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.connections[consumer]
}

func (s *StatisticStreamer) Save(method string, consumer string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, con := range s.connections {
		byConsumer := con.Stat.ByConsumer[consumer]
		con.Stat.ByConsumer[consumer] = byConsumer + 1

		byMethod := con.Stat.ByMethod[method]
		con.Stat.ByMethod[method] = byMethod + 1
	}
}

func (s *StatisticStreamer) Clear(consumer string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	con, has := s.connections[consumer]
	if !has {
		return
	}

	con.Clear()
}

func (s *StatisticStreamer) AddConnection(consumer string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, has := s.connections[consumer]
	if has {
		fmt.Printf("consumer=%s already has connection to statistic", consumer)
		return
	}

	con := NewStatConenction()
	s.connections[consumer] = con
}

func (s *StatisticStreamer) RemoveConnection(consumer string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.connections, consumer)
}

// todo: rename to LogStreamer?
type LogStreamer struct {
	connections map[string]chan *Event
	mu          sync.Mutex
}

func NewLogStreamer() *LogStreamer {
	return &LogStreamer{
		connections: make(map[string]chan *Event),
	}
}

func (l *LogStreamer) HasConnections() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	conns := len(l.connections)
	return conns > 0
}

func (l *LogStreamer) Send(e *Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, ch := range l.connections {
		ch <- e
	}
}

func (l *LogStreamer) GetCh(consumer string) <-chan *Event {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.connections[consumer]
}

func (l *LogStreamer) AddConnection(consumer string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	_, has := l.connections[consumer]
	if has {
		fmt.Printf("consumer=%s already has connection to logger", consumer)
		return
	}

	ch := make(chan *Event)

	l.connections[consumer] = ch
}

func (l *LogStreamer) RemoveConnection(consumer string) {
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
	LogStreamer  *LogStreamer
	StatStreamer *StatisticStreamer
	Host         string
	UnimplementedAdminServer
}

func NewAdminService(host string) AdminService {
	return AdminService{
		LogStreamer:  NewLogStreamer(),
		StatStreamer: NewStatStreamer(),
		Host:         host,
	}
}

func (s AdminService) Logging(params *Nothing, srv Admin_LoggingServer) error {
	ctx := srv.Context()
	consumer, err := getConsumer(ctx)
	if err != nil {
		return err
	}

	s.LogStreamer.AddConnection(consumer)
	ch := s.LogStreamer.GetCh(consumer)

	go func() {
		<-ctx.Done()
		s.LogStreamer.RemoveConnection(consumer)
	}()

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
	ctx := srv.Context()

	consumer, err := getConsumer(ctx)
	if err != nil {
		return err
	}

	s.StatStreamer.AddConnection(consumer)

	ch := make(chan *Stat)

	go func(ch chan *Stat) {
		timer := time.NewTicker(time.Duration(params.IntervalSeconds) * time.Second)

		defer func() {
			timer.Stop()
			s.StatStreamer.RemoveConnection(consumer)
			close(ch)
		}()

		for {
			select {
			case <-timer.C:
				stat := s.StatStreamer.GetConnection(consumer)
				ch <- stat.Stat
				s.StatStreamer.Clear(consumer)
			case <-ctx.Done():
				return
			}
		}
	}(ch)

	for stat := range ch {
		err := srv.Send(stat)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
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
		if !s.LogStreamer.HasConnections() {
			return handler(ctx, req)
		}

		method := info.FullMethod
		consumer, _ := getConsumer(ctx)

		event := s.createEvent(consumer, method)
		s.LogStreamer.Send(event)

		return handler(ctx, req)
	}
}

func statInterceptor(s AdminService) func(
	context.Context,
	interface{},
	*grpc.UnaryServerInfo,
	grpc.UnaryHandler,
) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if !s.StatStreamer.HasConnections() {
			return handler(ctx, req)
		}

		method := info.FullMethod
		consumer, _ := getConsumer(ctx)

		s.StatStreamer.Save(method, consumer)

		return handler(ctx, req)
	}
}

func statInterceptorStream(s AdminService) func(interface{}, grpc.ServerStream, *grpc.StreamServerInfo, grpc.StreamHandler) error {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !s.StatStreamer.HasConnections() {
			return handler(srv, ss)
		}

		ctx := ss.Context()

		method := info.FullMethod
		consumer, _ := getConsumer(ctx)

		s.StatStreamer.Save(method, consumer)

		return handler(srv, ss)
	}
}

func logInterceptorStream(s AdminService) func(interface{}, grpc.ServerStream, *grpc.StreamServerInfo, grpc.StreamHandler) error {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !s.LogStreamer.HasConnections() {
			return handler(srv, ss)
		}

		ctx := ss.Context()

		method := info.FullMethod
		consumer, _ := getConsumer(ctx)

		event := s.createEvent(consumer, method)
		s.LogStreamer.Send(event)

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
		grpc.ChainUnaryInterceptor(authInterceptor(acl), logInterceptor(admService), statInterceptor(admService)),
		grpc.ChainStreamInterceptor(authInterceptorStream(acl), logInterceptorStream(admService), statInterceptorStream(admService)),
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
