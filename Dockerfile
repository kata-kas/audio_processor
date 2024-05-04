FROM golang:latest

WORKDIR /go/src/app

COPY . .

RUN go build -o main audiopub.go

HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 CMD [ "curl", "-f", "http://localhost:9123/health" ]

CMD ["./main"]