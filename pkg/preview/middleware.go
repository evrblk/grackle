package preview

import (
	"context"
	"errors"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/evrblk/evrblk-go/authn"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var (
	errUnauthenticated = status.Error(codes.Unauthenticated, "unauthenticated")
)

const (
	signatureKey = "evrblk-signature"
	apiKeyKey    = "evrblk-api-key-id"
	timestampKey = "evrblk-timestamp"
)

type apiKey struct {
	id   string
	body string
}

type AuthenticationMiddleware struct {
	authKeysPath string

	mu   sync.Mutex
	keys map[string]*apiKey
}

func (m *AuthenticationMiddleware) Unary(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		//log.Println("failed to load metadata")
		return nil, errUnauthenticated
	}

	if len(md.Get(signatureKey)) != 1 {
		//log.Println("no evrblk-signature key in metadata")
		return nil, errUnauthenticated
	}
	signature := md.Get(signatureKey)[0]

	if len(md.Get(apiKeyKey)) != 1 {
		//log.Println("no evrblk-api-key-id key in metadata")
		return nil, errUnauthenticated
	}
	apiKeyIdStr := md.Get(apiKeyKey)[0]

	if len(md.Get(timestampKey)) != 1 {
		//log.Println("no evrblk-timestamp key in metadata")
		return nil, errUnauthenticated
	}
	timestamp, err := strconv.Atoi(md.Get(timestampKey)[0])
	if err != nil {
		//log.Printf("failed to convert timestamp %s \n", md.Get(timestampKey)[0])
		return nil, errUnauthenticated
	}

	key, err := m.getApiKey(apiKeyIdStr)
	if err != nil {
		return nil, errUnauthenticated
	}

	err = m.verifySignature(req, key, signature, int64(timestamp))
	if err != nil {
		return nil, errUnauthenticated
	}

	return handler(ctx, req)
}

func (m *AuthenticationMiddleware) verifySignature(req interface{}, key *apiKey, signature string, timestamp int64) error {
	requestProto, ok := req.(proto.Message)
	if !ok {
		return errors.New("request does not implement proto.Message")
	}

	now := time.Now()

	if strings.HasPrefix(key.id, "key_alfa_") {
		return authn.VerifyAlfaSignature(signature, timestamp, now, key.body, requestProto)
	} else if strings.HasPrefix(key.id, "key_bravo_") {
		date := authn.GetDateOfTimestamp(timestamp)
		hashedSecret, err := authn.HashBravoSecretWithDate(key.body, date)
		if err != nil {
			return err
		}

		return authn.VerifyBravoSignature(signature, timestamp, now, hashedSecret, requestProto)
	} else {
		return errors.New("unsupported api key type")
	}
}

func (m *AuthenticationMiddleware) getApiKey(apiKeyId string) (*apiKey, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key, ok := m.keys[apiKeyId]
	if ok {
		return key, nil
	}

	body, err := os.ReadFile(path.Join(m.authKeysPath, apiKeyId))
	if err != nil {
		return key, err
	}

	key = &apiKey{
		id:   apiKeyId,
		body: string(body),
	}

	m.keys[apiKeyId] = key

	return key, nil
}

func NewAuthenticationMiddleware(authKeysPath string) *AuthenticationMiddleware {
	return &AuthenticationMiddleware{
		keys:         make(map[string]*apiKey),
		authKeysPath: authKeysPath,
	}
}
