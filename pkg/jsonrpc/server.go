package jsonrpc

type Server struct {
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) Init(path string) error {
	return nil
}

func (s *Server) startServer() error {
	return nil
}
