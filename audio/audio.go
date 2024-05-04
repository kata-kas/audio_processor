package audio

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	sync "sync"
	"time"

	"github.com/acme/audiopub/messagebus"
	"github.com/acme/audiopub/security"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

var categoryMap = map[string]int{
	"hate":                   1,
	"hate/threatening":       2,
	"harassment":             3,
	"harassment/threatening": 4,
	"self-harm":              5,
	"self-harm/intent":       6,
	"self-harm/instructions": 7,
	"sexual":                 8,
	"sexual/minors":          9,
	"violence":               10,
	"violence/graphic":       11,
}

var priorityMap = map[string]string{
	"medium": "MEDIUM",
	"high":   "HIGH",
	"severe": "SEVERE",
}

type audioGRPCServer struct {
	sqsClient            *sqs.Client
	mb                   *messagebus.MessageBus
	deviceChannels       map[string]chan *AudioResponseEvent
	deviceChannelsLock   sync.RWMutex
	deviceProcessors     map[string]*AudioBatchProcessor
	deviceProcessorsLock sync.RWMutex
}

func getPriority(certainty int) string {
	if certainty <= 80 {
		return priorityMap["medium"]
	} else if certainty <= 90 {
		return priorityMap["high"]
	} else {
		return priorityMap["severe"]
	}
}

func (s *audioGRPCServer) AudioStream(stream AudioService_AudioStreamServer) error {
	var deviceID string

	defer func() {
		s.cleanupDeviceChannels(deviceID)
	}()

	for {
		select {
		case <-stream.Context().Done():
			// Client connection closed
			return nil
		default:
			// Continue processing audio events
			audioEvent, err := stream.Recv()
			if err != nil {
				log.Printf("error receiving audio event: %v", err)
				return err
			}

			if deviceID == "" {
				deviceID = audioEvent.DeviceId
				fmt.Printf("New stream for device: %s\n", deviceID)
			}

			token := audioEvent.GetToken()
			if token == "" {
				log.Println("token data is nil")
				continue
			}

			tokenData, err := security.GetTokenData(token)
			if err != nil {
				log.Printf("error getting token data: %v", err)
				continue
			}

			if !tokenData.IsSubscribed {
				fmt.Printf("token for %s is not subscribed\n", tokenData.Organization)
				continue
			}

			s.deviceChannelsLock.RLock()
			ch, ok := s.deviceChannels[audioEvent.DeviceId]
			s.deviceChannelsLock.RUnlock()

			if !ok {
				s.deviceChannelsLock.Lock()
				ch = make(chan *AudioResponseEvent)
				s.deviceChannels[audioEvent.DeviceId] = ch
				s.deviceChannelsLock.Unlock()
			}

			// Send audio event to processor asynchronously
			go s.processAudioEvent(audioEvent, tokenData, ch, stream)
		}
	}
}

func (s *audioGRPCServer) processAudioEvent(
	audioEvent *AudioEvent,
	tokenData *security.TokenData,
	ch chan *AudioResponseEvent,
	stream AudioService_AudioStreamServer,
) {
	audioProcessor := s.getOrCreateAudioProcessor(audioEvent.DeviceId, audioEvent, tokenData)
	audioProcessor.Add(audioEvent.AudioData)

	audioResponseEvent, ok := <-ch
	if !ok {
		return
	}

	if err := stream.Send(audioResponseEvent); err != nil {
		return
	}
}

func (s *audioGRPCServer) getOrCreateAudioProcessor(deviceID string, audioEvent *AudioEvent, tokenData *security.TokenData) *AudioBatchProcessor {
	s.deviceProcessorsLock.Lock()
	defer s.deviceProcessorsLock.Unlock()

	audioProcessor, ok := s.deviceProcessors[deviceID]
	if !ok {
		audioProcessor = NewAudioBatchProcessor(audioEvent, tokenData)
		s.deviceProcessors[deviceID] = audioProcessor
	}
	return audioProcessor
}

func (s *audioGRPCServer) cleanupDeviceChannels(deviceID string) {
	s.deviceChannelsLock.Lock()
	defer s.deviceChannelsLock.Unlock()

	if ch, ok := s.deviceChannels[deviceID]; ok {
		close(ch)
		delete(s.deviceChannels, deviceID)
		s.mb.Unsubscribe(deviceID)
	}

	s.deviceProcessorsLock.Lock()
	defer s.deviceProcessorsLock.Unlock()

	if abp, ok := s.deviceProcessors[deviceID]; ok {
		abp.Stop()
		delete(s.deviceProcessors, deviceID)
	}
}

func (s *audioGRPCServer) mustEmbedUnimplementedAudioServiceServer() {}

func StartResponseWorker(dataStreamer *audioGRPCServer, mb *messagebus.MessageBus) {
	newDevice := make(chan string)

	go func() {
		for deviceID := range newDevice {
			if !mb.IsSubscribed(deviceID) {
				ch := mb.Subscribe(deviceID)
				go processMessages(deviceID, ch, dataStreamer)
			}
		}
	}()

	// Main loop to listen for new devices in deviceChannels
	for {
		for deviceID := range dataStreamer.deviceChannels {
			select {
			case newDevice <- deviceID:
				// New device added, signal the goroutine to subscribe
			default:
				// Channel blocked, no new device added yet
			}
		}
		// Sleep briefly to avoid spinning too fast
		time.Sleep(1 * time.Second)
	}
}

func processMessages(deviceID string, ch chan messagebus.Message, dataStreamer *audioGRPCServer) {
	for {
		message := <-ch
		dataStreamer.deviceChannelsLock.RLock()
		deviceChannel, ok := dataStreamer.deviceChannels[deviceID]
		dataStreamer.deviceChannelsLock.RUnlock()
		if !ok {
			break
		}

		fmt.Printf("Received message for device %s: %v\n", deviceID, message)
		labels, err := getCategoryNumbers(message.Data.Categories)
		if err != nil {
			log.Printf("error getting category numbers: %v", err)
			continue
		}
		audioResponseEvent := &AudioResponseEvent{
			DeviceId: deviceID,
			Type:     message.Data.Type,
			Priority: getPriority(message.Data.Certainty),
			Labels:   labels,
		}

		deviceChannel <- audioResponseEvent
	}
}

func getCategoryNumbers(categories string) ([]int32, error) {
	categoryTitles := strings.Split(categories, ",")

	categoryNumbers := make([]int32, len(categoryTitles))

	for i, title := range categoryTitles {
		number, ok := categoryMap[title]
		if !ok {
			return nil, fmt.Errorf("category '%s' not found in map", title)
		}
		categoryNumbers[i] = int32(number)
	}

	return categoryNumbers, nil
}

func StartGRPCServer(mb *messagebus.MessageBus, ctx context.Context) {
	cfg := aws.Config{
		Region: os.Getenv("AWS_REGION"),
	}

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	serverRegistrar := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     5 * time.Minute,
			Time:                  1 * time.Minute,
			Timeout:               20 * time.Minute,
			MaxConnectionAgeGrace: 5 * time.Minute,
		}),
		grpc.MaxRecvMsgSize(1024*1024*1024), // 1GB
	)
	healthcheck := health.NewServer()
	healthpb.RegisterHealthServer(serverRegistrar, healthcheck)
	service := &audioGRPCServer{
		sqsClient:        sqs.NewFromConfig(cfg),
		mb:               mb,
		deviceChannels:   make(map[string]chan *AudioResponseEvent),
		deviceProcessors: make(map[string]*AudioBatchProcessor),
	}
	RegisterAudioServiceServer(serverRegistrar, service)
	reflection.Register(serverRegistrar)

	log.Printf("gRPC server started on port 50051")

	go func() {
		next := healthpb.HealthCheckResponse_SERVING

		for {
			healthcheck.SetServingStatus("", next)

			if next == healthpb.HealthCheckResponse_SERVING {
				next = healthpb.HealthCheckResponse_NOT_SERVING
			} else {
				next = healthpb.HealthCheckResponse_SERVING
			}

			time.Sleep(time.Second * 5)
		}
	}()

	go StartResponseWorker(service, mb)

	if err := serverRegistrar.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

type SQSAudioEvent struct {
	AudioData    string              `json:"audio_data"`
	TokenData    *security.TokenData `json:"token_data"`
	DeviceID     string              `json:"device_id"`
	Token        string              `json:"token"`
	RoomID       string              `json:"room_id"`
	Timestamp    int64               `json:"timestamp"`
	Timezone     string              `json:"timezone"`
	BufferLength int                 `json:"buffer_length"`
	SampleRate   int                 `json:"sample_rate"`
}

type AudioBatchProcessor struct {
	mu             sync.Mutex
	dataBatch      chan []byte
	s3Uploader     *s3.Client
	sqsClient      *sqs.Client
	sqsUrl         string
	audioEvent     *AudioEvent
	tokenData      *security.TokenData
	batchSizeLimit int
	done           chan struct{}
}

func NewAudioBatchProcessor(audioEvent *AudioEvent, tokenData *security.TokenData) *AudioBatchProcessor {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(os.Getenv("AWS_REGION")))
	if err != nil {
		log.Fatalf("error loading AWS config: %v", err)
	}

	abp := &AudioBatchProcessor{
		dataBatch:      make(chan []byte),
		s3Uploader:     s3.NewFromConfig(cfg),
		sqsClient:      sqs.NewFromConfig(cfg),
		sqsUrl:         os.Getenv("SQS_AUDIO"),
		done:           make(chan struct{}),
		batchSizeLimit: 600 * 1024, // 600 KB,
		audioEvent:     audioEvent,
		tokenData:      tokenData,
	}

	go abp.processBatch()

	return abp
}

func (abp *AudioBatchProcessor) Add(audioData []byte) {
	select {
	case abp.dataBatch <- audioData:
	default:
		abp.reset()
	}
}

func (abp *AudioBatchProcessor) SetTokenData(tokenData *security.TokenData) {
	abp.mu.Lock()
	defer abp.mu.Unlock()

	abp.tokenData = tokenData
}

func (abp *AudioBatchProcessor) reset() {
	abp.mu.Lock()
	defer abp.mu.Unlock()

	abp.dataBatch = make(chan []byte)
}

func (abp *AudioBatchProcessor) processBatch() {
	var batch []byte

	audioObjectName := fmt.Sprintf("%s/violations/%s.wav", abp.audioEvent.DeviceId, uuid.New().String())
	for {
		select {
		case data := <-abp.dataBatch:
			batch = append(batch, data...)
			if len(batch) >= abp.batchSizeLimit {
				abp.uploadBatch(audioObjectName, batch)
				abp.sendSQSMessage(audioObjectName)
				batch = nil
			}
		case <-abp.done:
			if len(batch) > 0 {
				abp.uploadBatch(audioObjectName, batch)
				abp.sendSQSMessage(audioObjectName)
			}
			return
		}
	}
}

func (abp *AudioBatchProcessor) Stop() {
	abp.mu.Lock()
	defer abp.mu.Unlock()

	select {
	case <-abp.done:
		return
	default:
		close(abp.done)
	}

	<-abp.done
}

func (abp *AudioBatchProcessor) uploadBatch(audioObjectName string, data []byte) {
	_, err := abp.s3Uploader.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(os.Getenv("AWS_BUCKET")),
		Key:    aws.String(audioObjectName),
		Body:   bytes.NewReader(data),
		ACL:    "public-read-write",
	})
	if err != nil {
		log.Printf("error uploading audio to s3: %v", err)
		return
	}
}

func (abp *AudioBatchProcessor) sendSQSMessage(audioObjectName string) {
	sqsObject := SQSAudioEvent{
		AudioData: audioObjectName,
		DeviceID:  abp.audioEvent.DeviceId,
		Token:     abp.audioEvent.Token,
		TokenData: abp.tokenData,
		Timestamp: abp.audioEvent.Timestamp,
	}
	sqsObjectJSON, err := json.Marshal(sqsObject)
	if err != nil {
		log.Printf("error marshaling sqs object: %v", err)
		return
	}

	deduplicationID := uuid.New().String()

	_, err = abp.sqsClient.SendMessage(context.TODO(), &sqs.SendMessageInput{
		MessageBody:            aws.String(string(sqsObjectJSON)),
		QueueUrl:               aws.String(abp.sqsUrl),
		MessageDeduplicationId: aws.String(deduplicationID),
		MessageGroupId:         aws.String(abp.audioEvent.DeviceId),
	})
	if err != nil {
		log.Printf("error sending sqs message: %v", err)
		return
	}
}
