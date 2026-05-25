package fileapi

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/kleffio/kleff-daemon/internal/application/ports"
)

// Server is a lightweight HTTP file-management API for workload data directories.
// It listens on a configurable port and is authenticated by a shared secret.
type Server struct {
	storagePath string
	sharedSecret string
	logger      ports.Logger
	srv         *http.Server
}

func NewServer(storagePath, sharedSecret string, port int, logger ports.Logger) *Server {
	s := &Server{
		storagePath:  storagePath,
		sharedSecret: sharedSecret,
		logger:       logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/{projectID}/{workloadID}/files", s.listOrStat)
	mux.HandleFunc("GET /v1/{projectID}/{workloadID}/files/download", s.download)
	mux.HandleFunc("GET /v1/{projectID}/{workloadID}/files/export", s.exportZip)
	mux.HandleFunc("POST /v1/{projectID}/{workloadID}/files/upload", s.upload)
	mux.HandleFunc("POST /v1/{projectID}/{workloadID}/files/rename", s.rename)
	mux.HandleFunc("POST /v1/{projectID}/{workloadID}/files/mkdir", s.mkdir)
	mux.HandleFunc("POST /v1/{projectID}/{workloadID}/files/import", s.importZip)
	mux.HandleFunc("DELETE /v1/{projectID}/{workloadID}/files", s.delete)

	s.srv = &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      s.auth(mux),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
	}
	return s
}

func (s *Server) Start() error {
	s.logger.Info("File API server listening", "addr", s.srv.Addr)
	if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

// auth middleware validates the Bearer token.
func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.sharedSecret == "" {
			http.Error(w, "server misconfigured: no shared secret", http.StatusInternalServerError)
			return
		}
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if token != s.sharedSecret {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// workloadRoot returns the absolute root directory for a workload.
// Must match the naming convention used by the docker adapter: kleff_proj_{shortID(projectID)}_{workloadID}.
func (s *Server) workloadRoot(projectID, workloadID string) string {
	return filepath.Join(s.storagePath, "kleff_proj_"+shortID(projectID)+"_"+workloadID)
}

// shortID strips dashes and truncates to 12 chars, matching the docker adapter's convention.
func shortID(s string) string {
	s = strings.ReplaceAll(s, "-", "")
	if len(s) > 12 {
		s = s[:12]
	}
	return s
}

// safePath resolves a relative path inside the workload root and rejects traversal.
func safePath(root, rel string) (string, error) {
	if rel == "" || rel == "/" {
		return root, nil
	}
	// Clean and make relative
	rel = filepath.FromSlash(rel)
	rel = filepath.Clean(rel)
	if filepath.IsAbs(rel) {
		rel = rel[1:]
	}
	abs := filepath.Join(root, rel)
	if !strings.HasPrefix(abs, root+string(filepath.Separator)) && abs != root {
		return "", fmt.Errorf("path traversal detected")
	}
	return abs, nil
}

type fileEntry struct {
	Name    string    `json:"name"`
	Path    string    `json:"path"`
	IsDir   bool      `json:"is_dir"`
	Size    int64     `json:"size"`
	ModTime time.Time `json:"mod_time"`
}

func (s *Server) listOrStat(w http.ResponseWriter, r *http.Request) {
	root, rel, ok := s.resolve(w, r)
	if !ok {
		return
	}
	target, err := safePath(root, rel)
	if err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	info, err := os.Stat(target)
	if err != nil {
		if os.IsNotExist(err) {
			// Workload directory doesn't exist yet (server not yet started).
			// Return an empty listing rather than 404 so the UI shows an empty file tree.
			if target == root {
				jsonOK(w, []fileEntry{})
				return
			}
			jsonError(w, "not found", http.StatusNotFound)
		} else {
			jsonError(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	if !info.IsDir() {
		// Return single file stat.
		jsonOK(w, fileEntry{
			Name:    info.Name(),
			Path:    rel,
			IsDir:   false,
			Size:    info.Size(),
			ModTime: info.ModTime().UTC(),
		})
		return
	}

	entries, err := os.ReadDir(target)
	if err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].IsDir() != entries[j].IsDir() {
			return entries[i].IsDir()
		}
		return entries[i].Name() < entries[j].Name()
	})

	items := make([]fileEntry, 0, len(entries))
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		entryRel := filepath.ToSlash(filepath.Join(rel, e.Name()))
		if !strings.HasPrefix(entryRel, "/") {
			entryRel = "/" + entryRel
		}
		items = append(items, fileEntry{
			Name:    e.Name(),
			Path:    entryRel,
			IsDir:   e.IsDir(),
			Size:    info.Size(),
			ModTime: info.ModTime().UTC(),
		})
	}
	jsonOK(w, items)
}

func (s *Server) download(w http.ResponseWriter, r *http.Request) {
	root, rel, ok := s.resolve(w, r)
	if !ok {
		return
	}
	target, err := safePath(root, rel)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	info, err := os.Stat(target)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if info.IsDir() {
		http.Error(w, "path is a directory", http.StatusBadRequest)
		return
	}

	ct := mime.TypeByExtension(filepath.Ext(target))
	if ct == "" {
		ct = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, info.Name()))
	http.ServeFile(w, r, target)
}

func (s *Server) upload(w http.ResponseWriter, r *http.Request) {
	root, rel, ok := s.resolve(w, r)
	if !ok {
		return
	}
	destDir, err := safePath(root, rel)
	if err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := os.MkdirAll(destDir, 0755); err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := r.ParseMultipartForm(64 << 20); err != nil {
		jsonError(w, "parse multipart: "+err.Error(), http.StatusBadRequest)
		return
	}
	files := r.MultipartForm.File["files"]
	if len(files) == 0 {
		jsonError(w, "no files in request", http.StatusBadRequest)
		return
	}

	for _, fh := range files {
		src, err := fh.Open()
		if err != nil {
			jsonError(w, "open upload: "+err.Error(), http.StatusInternalServerError)
			return
		}
		dest := filepath.Join(destDir, filepath.Base(fh.Filename))
		// Prevent traversal via filename.
		if !strings.HasPrefix(dest, root) {
			src.Close()
			jsonError(w, "invalid filename", http.StatusBadRequest)
			return
		}
		f, err := os.Create(dest)
		if err != nil {
			src.Close()
			jsonError(w, "create file: "+err.Error(), http.StatusInternalServerError)
			return
		}
		_, copyErr := io.Copy(f, src)
		f.Close()
		src.Close()
		if copyErr != nil {
			jsonError(w, "write file: "+copyErr.Error(), http.StatusInternalServerError)
			return
		}
	}
	jsonOK(w, map[string]any{"uploaded": len(files)})
}

func (s *Server) rename(w http.ResponseWriter, r *http.Request) {
	root := s.workloadRoot(r.PathValue("projectID"), r.PathValue("workloadID"))
	var body struct {
		From string `json:"from"`
		To   string `json:"to"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		jsonError(w, "decode body: "+err.Error(), http.StatusBadRequest)
		return
	}
	src, err := safePath(root, body.From)
	if err != nil {
		jsonError(w, "invalid from path", http.StatusBadRequest)
		return
	}
	dst, err := safePath(root, body.To)
	if err != nil {
		jsonError(w, "invalid to path", http.StatusBadRequest)
		return
	}
	if err := os.Rename(src, dst); err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonOK(w, map[string]any{"ok": true})
}

func (s *Server) mkdir(w http.ResponseWriter, r *http.Request) {
	root, rel, ok := s.resolve(w, r)
	if !ok {
		return
	}
	target, err := safePath(root, rel)
	if err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := os.MkdirAll(target, 0755); err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonOK(w, map[string]any{"ok": true})
}

func (s *Server) delete(w http.ResponseWriter, r *http.Request) {
	root, rel, ok := s.resolve(w, r)
	if !ok {
		return
	}
	if rel == "" || rel == "/" {
		jsonError(w, "cannot delete workload root", http.StatusBadRequest)
		return
	}
	target, err := safePath(root, rel)
	if err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := os.RemoveAll(target); err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonOK(w, map[string]any{"ok": true})
}

func (s *Server) exportZip(w http.ResponseWriter, r *http.Request) {
	root, rel, ok := s.resolve(w, r)
	if !ok {
		return
	}
	target, err := safePath(root, rel)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, err := os.Stat(target); err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", `attachment; filename="export.zip"`)

	zw := zip.NewWriter(w)
	defer zw.Close()

	_ = filepath.WalkDir(target, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(target, path)
		fw, err := zw.Create(filepath.ToSlash(rel))
		if err != nil {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(fw, f)
		return err
	})
}

func (s *Server) importZip(w http.ResponseWriter, r *http.Request) {
	root, rel, ok := s.resolve(w, r)
	if !ok {
		return
	}
	destDir, err := safePath(root, rel)
	if err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := os.MkdirAll(destDir, 0755); err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := r.ParseMultipartForm(256 << 20); err != nil {
		jsonError(w, "parse multipart: "+err.Error(), http.StatusBadRequest)
		return
	}
	fhs := r.MultipartForm.File["file"]
	if len(fhs) == 0 {
		jsonError(w, "no file in request", http.StatusBadRequest)
		return
	}
	fh := fhs[0]
	src, err := fh.Open()
	if err != nil {
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer src.Close()

	zr, err := zip.NewReader(src.(io.ReaderAt), fh.Size)
	if err != nil {
		jsonError(w, "invalid zip: "+err.Error(), http.StatusBadRequest)
		return
	}

	for _, f := range zr.File {
		dest := filepath.Join(destDir, filepath.FromSlash(f.Name))
		// Zip-slip prevention.
		if !strings.HasPrefix(dest, destDir+string(filepath.Separator)) && dest != destDir {
			jsonError(w, "zip slip detected", http.StatusBadRequest)
			return
		}
		if f.FileInfo().IsDir() {
			_ = os.MkdirAll(dest, 0755)
			continue
		}
		_ = os.MkdirAll(filepath.Dir(dest), 0755)
		rc, err := f.Open()
		if err != nil {
			jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		out, err := os.Create(dest)
		if err != nil {
			rc.Close()
			jsonError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, copyErr := io.Copy(out, rc)
		out.Close()
		rc.Close()
		if copyErr != nil {
			jsonError(w, copyErr.Error(), http.StatusInternalServerError)
			return
		}
	}
	jsonOK(w, map[string]any{"ok": true})
}

// resolve extracts and validates the workload root + "path" query param.
func (s *Server) resolve(w http.ResponseWriter, r *http.Request) (root, rel string, ok bool) {
	root = s.workloadRoot(r.PathValue("projectID"), r.PathValue("workloadID"))
	rel = r.URL.Query().Get("path")
	ok = true
	return
}

func jsonOK(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func jsonError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
