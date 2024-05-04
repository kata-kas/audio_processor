PROTOC := protoc

PROTO_SRC_DIR := ./proto

OUT_DIR := ./internal/audio

PROTO_FILES := $(wildcard $(PROTO_SRC_DIR)/*.proto)

generate_proto:
    $(PROTOC) --proto_path=$(PROTO_SRC_DIR) --go_out=$(OUT_DIR) --go_opt=paths=source_relative $(PROTO_FILES)

.PHONY: generate_proto