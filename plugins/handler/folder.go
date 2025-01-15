package handler

import (
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const indexPage = "/index.html"

// folder copied from Turna's folder middleware to serve folder.
type folder struct {
	// Path is the path to the folder
	Path string `cfg:"path"`
	// Index is automatically redirect to index.html
	Index bool `cfg:"index"`
	// StripIndexName is strip index name from url
	StripIndexName bool `cfg:"strip_index_name"`
	// IndexName is the name of the index file, default is index.html
	IndexName string `cfg:"index_name"`
	// SPA is automatically redirect to index.html
	SPA bool `cfg:"spa"`
	// SPAEnableFile is enable .* file to be served to index.html if not found, default is false
	SPAEnableFile bool `cfg:"spa_enable_file"`
	// SPAIndex is set the index.html location, default is IndexName
	SPAIndex string `cfg:"spa_index"`
	// SPAIndexRegex set spa_index from URL path regex
	SPAIndexRegex []*RegexPathStore `cfg:"spa_index_regex"`
	// PrefixPath for strip prefix path for real file path
	PrefixPath string `cfg:"prefix_path"`
	// FilePathRegex is regex replacement for real file path, comes after PrefixPath apply
	// File path doesn't include / suffix
	FilePathRegex []*RegexPathStore `cfg:"file_path_regex"`

	CacheRegex []*RegexCacheStore `cfg:"cache_regex"`

	DisableFolderSlashRedirect bool `cfg:"disable_folder_slash_redirect"`

	fs http.FileSystem
}

func (f *folder) SetFs(fs http.FileSystem) {
	f.fs = fs
}

type RegexPathStore struct {
	Regex       string `cfg:"regex"`
	Replacement string `cfg:"replacement"`
	rgx         *regexp.Regexp
}

type RegexCacheStore struct {
	Regex        string `cfg:"regex"`
	CacheControl string `cfg:"cache_control"`
	rgx          *regexp.Regexp
}

func (f *folder) Handler() (http.Handler, error) {
	if f.IndexName == "" {
		f.IndexName = indexPage
	}

	f.IndexName = strings.Trim(f.IndexName, "/")

	if f.SPAIndex == "" {
		f.SPAIndex = f.IndexName
	}

	for i := range f.SPAIndexRegex {
		rgx, err := regexp.Compile(f.SPAIndexRegex[i].Regex)
		if err != nil {
			return nil, err
		}

		f.SPAIndexRegex[i].rgx = rgx
	}

	for i := range f.FilePathRegex {
		rgx, err := regexp.Compile(f.FilePathRegex[i].Regex)
		if err != nil {
			return nil, err
		}

		f.FilePathRegex[i].rgx = rgx
	}

	for i := range f.CacheRegex {
		rgx, err := regexp.Compile(f.CacheRegex[i].Regex)
		if err != nil {
			return nil, err
		}

		f.CacheRegex[i].rgx = rgx
	}

	if f.fs == nil {
		f.fs = http.Dir(f.Path)
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upath := r.URL.Path
		if !strings.HasPrefix(upath, "/") {
			upath = "/" + upath
		}

		cPath := path.Clean(upath)
		if f.PrefixPath != "" {
			prefix := strings.TrimSuffix(f.PrefixPath, "/")
			cPath = strings.TrimPrefix(cPath, prefix)
			if cPath == "" {
				cPath = "/"
			}
		}

		for _, r := range f.FilePathRegex {
			cPathOrg := cPath
			cPath = r.rgx.ReplaceAllString(cPath, r.Replacement)
			if cPath != cPathOrg {
				break
			}
		}

		if err := f.serveFile(w, r, upath, cPath); err != nil {
			httpResponseError(w, err)
		}
	}), nil
}

// name is '/'-separated, not filepath.Separator.
func (f *folder) serveFile(w http.ResponseWriter, r *http.Request, uPath, cPath string) error {
	// redirect .../index.html to .../
	// can't use Redirect() because that would make the path absolute,
	// which would be a problem running under StripPrefix
	if f.StripIndexName && strings.HasSuffix(uPath, f.IndexName) {
		return localRedirect(w, r, strings.TrimSuffix(uPath, f.IndexName))
	}

	file, err := f.fs.Open(cPath)
	if err != nil {
		if os.IsNotExist(err) && f.SPA {
			if f.SPAEnableFile || !strings.Contains(filepath.Base(cPath), ".") {
				for _, spaR := range f.SPAIndexRegex {
					spaFile := spaR.rgx.ReplaceAllString(uPath, spaR.Replacement)
					if spaFile != uPath {
						return f.fsFile(w, r, spaFile)
					}
				}

				return f.fsFile(w, r, f.SPAIndex)
			}
		}

		return toHTTPError(err)
	}
	defer file.Close()

	d, err := file.Stat()
	if err != nil {
		return toHTTPError(err)
	}

	// redirect to canonical path: / at end of directory url
	// r.URL.Path always begins with /
	if d.IsDir() {
		if uPath[len(uPath)-1] != '/' {
			if !f.DisableFolderSlashRedirect {
				return localRedirect(w, r, path.Base(uPath)+"/")
			}
		}
	} else {
		if uPath[len(uPath)-1] == '/' {
			return localRedirect(w, r, "../"+path.Base(uPath))
		}
	}

	if d.IsDir() && f.Index {
		// use contents of index.html for directory, if present
		ff, err := f.fs.Open(filepath.Join(cPath, f.IndexName))
		if err == nil {
			defer ff.Close()
			dd, err := ff.Stat()
			if err == nil {
				d = dd
				file = ff
			}
		}
	}

	// Still a directory? (we didn't find an index.html file)
	if d.IsDir() {
		return toHTTPError(os.ErrNotExist)
	}

	return f.ServeContent(w, r, d.Name(), d.ModTime(), file)
}

// localRedirect gives a Moved Permanently response.
// It does not convert relative paths to absolute paths like Redirect does.
func localRedirect(w http.ResponseWriter, r *http.Request, newPath string) error {
	if q := r.URL.RawQuery; q != "" {
		newPath += "?" + q
	}

	w.Header().Set("Location", newPath)
	w.WriteHeader(http.StatusTemporaryRedirect)

	return nil
}

// toHTTPError returns a non-specific HTTP error message for the given error.
func toHTTPError(err error) error {
	if os.IsNotExist(err) {
		return NewError("", nil, http.StatusNotFound)
	}
	if os.IsPermission(err) {
		return NewError("", nil, http.StatusForbidden)
	}

	return err
}

func (f *folder) fsFile(w http.ResponseWriter, r *http.Request, file string) error {
	hFile, err := f.fs.Open(file)
	if err != nil {
		return err
	}
	defer hFile.Close()

	fi, err := hFile.Stat()
	if err != nil {
		return err
	}

	return f.ServeContent(w, r, fi.Name(), fi.ModTime(), hFile)
}

func (f *folder) ServeContent(w http.ResponseWriter, req *http.Request, name string, modtime time.Time, content io.ReadSeeker) error {
	for _, r := range f.CacheRegex {
		if r.rgx.MatchString(name) {
			w.Header().Set("Cache-Control", r.CacheControl)

			break
		}
	}

	http.ServeContent(w, req, name, modtime, content)

	return nil
}
