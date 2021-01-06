package zip_streamer

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/minio/minio-go/v7"

	"github.com/cloudfoundry-community/gautocloud"

	"github.com/google/uuid"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/philips-software/gautocloud-connectors/hsdp"
)

type zipEntry struct {
	S3Path  string
	ZipPath string
}

type s3Creds struct {
	AccessKey    string `json:"accessKey"`
	SecretKey    string `json:"secretKey"`
	SessionToken string `json:"sessionToken"`
	Bucket       string `json:"bucket"`
	Endpoint     string `json:"endpoint"`
}

type zipPayload struct {
	S3Creds     s3Creds    `json:"s3Creds"`
	ZipFilename string     `json:"zipFilename"`
	Entries     []zipEntry `json:"entries"`
}

func UnmarshalPayload(payload []byte) (*s3Creds, string, []*FileEntry, error) {
	var parsed zipPayload
	err := json.Unmarshal(payload, &parsed)
	if err != nil {
		return nil, "", nil, err
	}
	zipFilename := "archive.zip"
	if parsed.ZipFilename != "" {
		zipFilename = parsed.ZipFilename
	}

	results := make([]*FileEntry, 0)
	for _, entry := range parsed.Entries {
		fileEntry, err := NewFileEntry(entry.S3Path, entry.ZipPath)
		if err == nil {
			results = append(results, fileEntry)
		}
	}

	return &parsed.S3Creds, zipFilename, results, nil
}

type Config struct {
	Username string
	Password string
}

type Server struct {
	router       *mux.Router
	linkCache    LinkCache
	config       Config
	directClient *hsdp.S3MinioClient
}

func basicAuthWrapper(config Config, originalHandler http.HandlerFunc) http.HandlerFunc {
	// If no username and password are set, don't wrap
	if config.Username == "" || config.Password == "" {
		return originalHandler
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if username, password, ok := r.BasicAuth(); !ok ||
			!(username == config.Username && password == config.Password) {
			w.Header().Set("WWW-Authenticate", `Basic realm="s3zipstreamer"`)
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("Unauthorized\n"))
			return
		}
		originalHandler(w, r)
	}
}

func NewServer(config Config) (*Server, error) {
	r := mux.NewRouter()

	timeout := time.Second * 60
	server := Server{
		router:    r,
		linkCache: NewLinkCache(&timeout),
		config:    config,
	}
	// Map direct client if there is a binding with S3 service
	_ = gautocloud.Inject(&server.directClient)

	r.HandleFunc("/download", basicAuthWrapper(config, server.HandlePostDownload)).Methods("POST")
	r.HandleFunc("/create_download_link", basicAuthWrapper(config, server.HandleCreateLink)).Methods("POST")
	r.HandleFunc("/download_link/{link_id}", server.HandleDownloadLink).Methods("GET")

	return &server, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	originsOk := handlers.AllowedOrigins([]string{"*"})
	headersOk := handlers.AllowedHeaders([]string{"Content-Type", "X-Requested-With", "*"})
	methodsOk := handlers.AllowedMethods([]string{"GET", "HEAD", "POST", "PUT", "OPTIONS"})
	handlers.CORS(originsOk, headersOk, methodsOk)(s.router).ServeHTTP(w, r)
}

func (s *Server) HandleCreateLink(w http.ResponseWriter, req *http.Request) {
	s3Creds, fileName, fileEntries, err := s.parseZipRequest(w, req)
	if err != nil {
		return
	}

	linkId := uuid.New().String()
	s.linkCache.Set(linkId, cacheEntry{
		Filename: fileName,
		S3Creds:  *s3Creds,
		Entries:  fileEntries,
	})
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok","link_id":"` + linkId + `"}`))
}

func (s *Server) parseZipRequest(w http.ResponseWriter, req *http.Request) (*s3Creds, string, []*FileEntry, error) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"status":"error","error":"missing body"}`))
		return nil, "", nil, err
	}

	s3Creds, fileName, fileEntries, err := UnmarshalPayload(body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"status":"error","error":"invalid body"}`))
		return nil, "", nil, err
	}

	return s3Creds, fileName, fileEntries, nil
}

func (s *Server) HandlePostDownload(w http.ResponseWriter, req *http.Request) {
	s3Creds, fileName, fileEntries, err := s.parseZipRequest(w, req)
	if err != nil {
		return
	}

	s.streamEntries(&cacheEntry{
		Filename: fileName,
		S3Creds:  *s3Creds,
		Entries:  fileEntries,
	}, w, req)
}

func (s *Server) HandleDownloadLink(w http.ResponseWriter, req *http.Request) {
	linkId := mux.Vars(req)["link_id"]
	cacheEntry := s.linkCache.Get(linkId)
	if cacheEntry == nil || cacheEntry.Entries == nil {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"status":"error","error":"link not found"}`))
		return
	}

	s.streamEntries(cacheEntry, w, req)
}

func (s *Server) streamEntries(entry *cacheEntry, w http.ResponseWriter, req *http.Request) {
	zipStreamer, err := NewZipStream(entry.Entries, w)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"status":"error","error":"invalid entries"}`))
		return
	}

	// need to write the header before bytes
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", "attachment; filename=\""+entry.Filename+"\"")
	w.WriteHeader(http.StatusOK)
	if s.directClient != nil {
		fmt.Printf("using direct client for streaming...\n")
		err = zipStreamer.StreamAllFiles(s.directClient.Client, s.directClient.Bucket)
	} else { // S3Creds based
		fmt.Printf("using S3Creds client for streaming...\n")
		s3creds := entry.S3Creds
		s3credsClient, err := minio.New(s3creds.Endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(s3creds.AccessKey, s3creds.SecretKey, s3creds.SessionToken),
			Secure: true,
		})
		if err != nil {
			fmt.Printf("error streaming: %v\n", err)
			closeForError(w)
			return
		}
		err = zipStreamer.StreamAllFiles(s3credsClient, s3creds.Bucket)
	}

	if err != nil {
		// Close the connection so the client gets an error instead of 200 but invalid file
		fmt.Printf("error streaming: %v\n", err)
		closeForError(w)
	}
}

func closeForError(w http.ResponseWriter) {
	hj, ok := w.(http.Hijacker)

	if !ok {
		return
	}

	conn, _, err := hj.Hijack()
	if err != nil {
		return
	}

	conn.Close()
}
