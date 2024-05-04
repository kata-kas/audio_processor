package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/acme/audiopub/audio"
	messagebus "github.com/acme/audiopub/messagebus"
	"github.com/acme/audiopub/queue"
	"github.com/acme/audiopub/security"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/joho/godotenv"
)

type Handler struct{}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (h Handler) Health(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (h Handler) GetCertificate(w http.ResponseWriter, r *http.Request) {
	certificate, err := security.GetCertificate()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-x509-ca-cert")
	w.WriteHeader(http.StatusOK)
	w.Write(*certificate)
}

func init() {
	Load(".env")
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(os.Getenv("AWS_REGION")))
	if err != nil {
		log.Fatalf("error loading AWS config: %v", err)
	}
	mb := messagebus.NewMessageBus()

	subscriber, subErr := queue.NewSubscriber(cfg, mb, os.Getenv("ARN_TOPIC"))
	if subErr != nil {
		fmt.Println(subErr)
		log.Fatalf("error creating subscriber: %v", err)
	}
	done := make(chan struct{})
	defer close(done)
	defer subscriber.DeleteQueueAndSubscription()

	go subscriber.StartListening()
	go setupAPI(done, ctx)
	go audio.StartGRPCServer(mb, ctx)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	<-sig
	log.Printf("Received termination signal, shutting down...")
	cancel()

	log.Printf("All services have been shut down")
}

func setupAPI(done <-chan struct{}, parentCtx context.Context) {
	handler := Handler{}
	http.HandleFunc("/health", Handler{}.Health)
	http.HandleFunc("/certificate", Handler{}.GetCertificate)

	makeServer := func() *http.Server {
		return &http.Server{
			Handler: handler,
		}
	}

	srv := makeServer()
	serve(parentCtx, srv, done)
}

func serve(ctx context.Context, srv *http.Server, done <-chan struct{}) {
	go func() {
		select {
		case <-done:
			if err := srv.Shutdown(ctx); err != nil {
				log.Fatalf("HTTP server shutdown error: %v", err)
			}
		case <-ctx.Done():
			if err := srv.Shutdown(ctx); err != nil {
				log.Fatalf("HTTP server shutdown error: %v", err)
			}
			log.Printf("HTTP server shutdown")
		}
	}()

	if err := http.ListenAndServe(":9123", nil); err != nil {
		log.Fatalf("HTTP server error: %v", err)
	}

	log.Printf("HTTP server started on port 9123")
}

// Load loads the environment variables from the .env file.
func Load(envFile string) {
	err := godotenv.Load(dir(envFile))
	if err != nil {
		fmt.Printf("Error loading .env file: %v", err)
	}
}

// dir returns the absolute path of the given environment file (envFile) in the Go module's
// root directory. It searches for the 'go.mod' file from the current working directory upwards
// and appends the envFile to the directory containing 'go.mod'.
// It panics if it fails to find the 'go.mod' file.
func dir(envFile string) string {
	currentDir, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	for {
		goModPath := filepath.Join(currentDir, "go.mod")
		if _, err := os.Stat(goModPath); err == nil {
			break
		}

		parent := filepath.Dir(currentDir)
		if parent == currentDir {
			panic(fmt.Errorf("go.mod not found"))
		}
		currentDir = parent
	}

	return filepath.Join(currentDir, envFile)
}
