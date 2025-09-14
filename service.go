package main

import (
	"context"
	"encoding/json"
	"net"
	"strings"

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
2. Метод логирования
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

type AdminService struct {
	UnimplementedAdminServer
}

func NewAdminService() AdminService {
	return AdminService{}
}

func (s AdminService) Logging(params *Nothing, srv Admin_LoggingServer) error {
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
	md, _ := metadata.FromIncomingContext(ctx)
	err := status.Error(codes.Unauthenticated, "Unauthenticated")

	consumer := md.Get("consumer")
	if len(consumer) <= 0 {
		return err
	}

	allowed := false
	for _, c := range consumer {
		methods, ok := acl[c]
		if !ok {
			continue
		}

		if checkAllowedMethods(methods, fullMethod) {
			allowed = true
			break
		}
	}

	if !allowed {
		return err
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

	server := grpc.NewServer(
		grpc.UnaryInterceptor(authInterceptor(acl)),
		grpc.StreamInterceptor(authInterceptorStream(acl)),
	)

	admService := NewAdminService()
	bizService := NewBizService()

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
