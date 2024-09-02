package protobuf

import (
	"fmt"
	"github.com/jhump/protoreflect/desc/protoparse"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"kafka-protobuf-cli/src/util"
)

func LoadProtoFile(protoPath string) (protoreflect.FileDescriptor, error) {
	parser := protoparse.Parser{}

	fileDescriptors, err := parser.ParseFiles(protoPath)
	if err != nil {
		return nil, err
	}

	fileDescriptor, err := util.GetFirstElement(fileDescriptors)
	if err != nil {
		return nil, err
	}

	fd, err := protodesc.NewFile(fileDescriptor.AsFileDescriptorProto(), protoregistry.GlobalFiles)
	if err != nil {
		return nil, err
	}
	return fd, nil
}

func GetMessageDescriptor(fileDescriptor protoreflect.FileDescriptor, messageName string) (protoreflect.MessageDescriptor, error) {
	messages := fileDescriptor.Messages()
	for i := 0; i < messages.Len(); i++ {
		message := messages.Get(i)
		if string(message.FullName()) == messageName {
			return message, nil
		}
	}

	return nil, fmt.Errorf("message type %s not found", messageName)
}

func UnmarshalMessageFromProto(path string, messageName string) (protoreflect.MessageDescriptor, error) {
	fileDescriptor, err := LoadProtoFile(path)
	if err != nil {
		return nil, err
	}

	if err := protoregistry.GlobalFiles.RegisterFile(fileDescriptor); err != nil {
		return nil, err
	}

	return GetMessageDescriptor(fileDescriptor, messageName)
}
