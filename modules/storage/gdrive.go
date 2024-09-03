// Copyright 2024 The Gitea Authors. All rights reserved.
// SPDX-License-Identifier: MIT

package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"strings"
	"time"

	"code.gitea.io/gitea/modules/cache"
	"code.gitea.io/gitea/modules/json"
	"code.gitea.io/gitea/modules/log"
	"code.gitea.io/gitea/modules/setting"

	"golang.org/x/oauth2"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
)

const (
	GoogleDriveMimeFolder   string = "application/vnd.google-apps.folder"                // Folder mime type
	GoogleDriveMimeSyslink  string = "application/vnd.google-apps.shortcut"              // Syslink mime type
	GoogleListQueryWithName string = "trashed=false and '%s' in parents and name = '%s'" // Query files list with name
	GoogleListQuery         string = "trashed=false and '%s' in parents"                 // Query files list
)

type Gdrive struct {
	GoogleConfig *oauth2.Config // Google client app oauth project
	GoogleToken  *oauth2.Token  // Authenticated user
	driveService *drive.Service // Google drive service
	rootDrive    *drive.File    // Root to find files
}

func init() {
	RegisterStorageType(setting.GoogleDriveType, NewGoogleDrive)
}

// Create new Gdrive struct and configure google drive client
func NewGoogleDrive(ctx context.Context, cfg *setting.Storage) (ObjectStorage, error) {
	gdrive := &Gdrive{
		GoogleConfig: &oauth2.Config{
			ClientID:     cfg.GoogleDriveConfig.Client,
			ClientSecret: cfg.GoogleDriveConfig.Secret,
			RedirectURL:  cfg.GoogleDriveConfig.Redirect,
			Scopes:       []string{drive.DriveScope, drive.DriveFileScope},
			Endpoint: oauth2.Endpoint{
				AuthURL:  cfg.GoogleDriveConfig.AuthURI,
				TokenURL: cfg.GoogleDriveConfig.TokenURI,
			},
		},
		GoogleToken: &oauth2.Token{
			AccessToken:  cfg.GoogleDriveConfig.AccessToken,
			TokenType:    cfg.GoogleDriveConfig.TokenType,
			RefreshToken: cfg.GoogleDriveConfig.RefreshToken,
			Expiry:       cfg.GoogleDriveConfig.Expire,
		},
	}

	var err error
	if gdrive.driveService, err = drive.NewService(ctx, option.WithHTTPClient(gdrive.GoogleConfig.Client(ctx, gdrive.GoogleToken))); err != nil {
		return nil, err
	}

	if cfg.GoogleDriveConfig.RootFolder != "" {
		n := strings.Split(cfg.GoogleDriveConfig.RootFolder, "/")
		// Create folder with root id
		if strings.HasPrefix(n[0], "gdrive:") {
			if gdrive.rootDrive, err = gdrive.driveService.Files.Get(n[0][7:]).Fields("*").Do(); err != nil {
				return nil, fmt.Errorf("cannot get root: %v", err)
			}
			n = n[1:]
		} else if gdrive.rootDrive, err = gdrive.MkdirAll(strings.Join(n, "/")); err != nil {
			return nil, err
		}

		// resolve and create path not exists in new root
		if len(n) >= 1 {
			if gdrive.rootDrive, err = gdrive.MkdirAll(strings.Join(n, "/")); err != nil {
				return nil, err
			}
		}
	} else if gdrive.rootDrive, err = gdrive.driveService.Files.Get("root").Fields("*").Do(); err != nil {
		return nil, fmt.Errorf("cannot get root: %v", err)
	}

	log.Debug("gdrive: root folder name %q, id %q", gdrive.rootDrive.Name, gdrive.rootDrive.Id)
	return gdrive, nil
}

func (gdrive *Gdrive) cacheDelete(path string) error {
	if cc := cache.GetCache(); cc != nil {
		return cc.Delete(fmt.Sprintf("gdrive:%s:%s", gdrive.rootDrive.Id, fixPath(path)))
	}
	return nil
}

func (gdrive *Gdrive) cacheGet(path string) *drive.File {
	if cc := cache.GetCache(); cc != nil {
		if str, ok := cc.Get(fmt.Sprintf("gdrive:%s:%s", gdrive.rootDrive.Id, fixPath(path))); ok && str != "" {
			var node drive.File
			if err := json.Unmarshal([]byte(str), &node); err != nil {
				return nil
			} else if node.Id == "" {
				return nil
			}
			log.Debug("Gdrive cache get: %s", str)
			return &node
		}
	}
	return nil
}

func (gdrive *Gdrive) cachePut(path string, node *drive.File) error {
	if cc := cache.GetCache(); cc != nil {
		body, err := json.Marshal(node)
		if err != nil {
			return err
		}
		log.Debug("Gdrive cache put: %s", string(body))
		return cc.Put(fmt.Sprintf("gdrive:%s:%s", gdrive.rootDrive.Id, fixPath(path)), string(body), 60*24*4)
	}
	return nil
}

func (*Gdrive) URL(path, name string) (*url.URL, error) {
	return nil, ErrURLNotSupported
}

// Get Node info and is not trashed/deleted
func (gdrive *Gdrive) resolveNode(folderID, name string) (*drive.File, error) {
	name = strings.ReplaceAll(strings.ReplaceAll(name, `\`, `\\`), `'`, `\'`)
	file, err := gdrive.driveService.Files.List().Fields("*").PageSize(300).Q(fmt.Sprintf(GoogleListQueryWithName, folderID, name)).Do()
	if err != nil {
		return nil, err
	} else if len(file.Files) != 1 {
		return nil, fs.ErrNotExist
	} else if len(file.Files) == 0 {
		return nil, fs.ErrNotExist
	} else if file.Files[0].Trashed {
		return file.Files[0], fs.ErrNotExist
	}
	return file.Files[0], nil
}

// List all files in folder
func (gdrive *Gdrive) listNodes(folderID string) ([]*drive.File, error) {
	var nodes []*drive.File
	folderGdrive := gdrive.driveService.Files.List().Fields("*").Q(fmt.Sprintf(GoogleListQuery, folderID)).PageSize(1000)
	for {
		res, err := folderGdrive.Do()
		if err != nil {
			return nodes, err
		}
		nodes = append(nodes, res.Files...)
		if res.NextPageToken == "" {
			break
		}
		folderGdrive.PageToken(res.NextPageToken)
	}
	return nodes, nil
}

// Split node [["node", "node"]]
func pathSplit(path string) [][2]string {
	path = strings.TrimPrefix(strings.TrimSuffix(strings.ReplaceAll(strings.ReplaceAll(path, `\\`, "/"), `\`, "/"), "/"), "/")
	var nodes [][2]string
	lastNode := 0
	for indexStr := range path {
		if path[indexStr] == '/' {
			nodes = append(nodes, [2]string{path[lastNode:indexStr], path[0:indexStr]})
			lastNode = indexStr + 1
		}
	}
	nodes = append(nodes, [2]string{path[lastNode:], path})
	return nodes
}

func checkMkdir(path string) bool   { return len(pathSplit(path)) > 1 }
func getLast(path string) [2]string { n := pathSplit(path); return n[len(n)-1] }
func fixPath(path string) string    { return getLast(path)[1] }

func (gdrive *Gdrive) GetNode(path string) (*drive.File, error) {
	nodes := pathSplit(path)
	current := gdrive.cacheGet(nodes[len(nodes)-1][1])
	if current == nil {
		current = gdrive.rootDrive
		for _, nodeData := range nodes {
			previus := current
			if current = gdrive.cacheGet(nodeData[1]); current == nil {
				var err error
				if current, err = gdrive.resolveNode(previus.Id, nodeData[0]); err != nil {
					return nil, err
				} else if err = gdrive.cachePut(nodeData[1], current); err != nil {
					return nil, err
				}
				continue
			}
		}
	}
	return current, nil
}

// Create recursive directory if not exists
func (gdrive *Gdrive) MkdirAll(path string) (*drive.File, error) {
	nodes := pathSplit(path)
	current := gdrive.cacheGet(nodes[len(nodes)-1][1])
	if current == nil {
		current = gdrive.rootDrive
		for nodeIndex, nodeData := range nodes {
			previus := current
			if current = gdrive.cacheGet(nodeData[1]); current == nil {
				var err error
				if current, err = gdrive.resolveNode(previus.Id, nodeData[0]); err != nil {
					if err == fs.ErrNotExist {
						var createNode drive.File
						createNode.MimeType = GoogleDriveMimeFolder
						for _, nodeData = range nodes[nodeIndex:] {
							createNode.Name = nodeData[0]             // Node name
							createNode.Parents = []string{previus.Id} // Root to create folder
							if current, err = gdrive.driveService.Files.Create(&createNode).Fields("*").Do(); err != nil {
								return nil, err
							} else if err = gdrive.cachePut(nodeData[1], current); err != nil {
								return nil, err
							}
						}
						return current, nil
					}
					return nil, err
				} else if err = gdrive.cachePut(nodeData[1], current); err != nil {
					return nil, err
				}
				continue
			}
		}
	}
	return current, nil
}

type driveStat struct {
	node *drive.File
	time time.Time
}

func (node *driveStat) Name() string      { return getLast(node.node.Name)[0] }
func (node *driveStat) Size() int64       { return node.node.Size }
func (node driveStat) ModTime() time.Time { return node.time }
func (node *driveStat) Sys() any          { return nil }
func (node *driveStat) IsDir() bool       { return node.node.MimeType == GoogleDriveMimeFolder }
func (node *driveStat) Mode() fs.FileMode {
	if node.node.MimeType == GoogleDriveMimeFolder {
		return fs.ModeDir | fs.ModePerm
	} else if node.node.MimeType == GoogleDriveMimeSyslink {
		return fs.ModeSymlink
	}
	return fs.ModePerm
}

func (gdrive *Gdrive) Stat(path string) (fs.FileInfo, error) {
	fileNode, err := gdrive.GetNode(path)
	if err != nil {
		return nil, err
	}
	stat := &driveStat{node: fileNode}
	if fileNode.ModifiedTime != "" {
		if err := stat.time.UnmarshalText([]byte(fileNode.ModifiedTime)); err != nil {
			return nil, err
		}
	} else {
		if err := stat.time.UnmarshalText([]byte(fileNode.CreatedTime)); err != nil {
			return nil, err
		}
	}
	return stat, nil
}

type driveOpen struct {
	nodeStat *driveStat
	client   *drive.Service
	nodeRes  *http.Response
	offset   int64
}

func (open driveOpen) Stat() (fs.FileInfo, error) { return open.nodeStat, nil }
func (open *driveOpen) Close() error {
	open.offset = 0
	return open.nodeRes.Body.Close()
}

func (open *driveOpen) Seek(offset int64, whence int) (int64, error) {
	node := open.client.Files.Get(open.nodeStat.node.Id).AcknowledgeAbuse(true)
	switch whence {
	case io.SeekStart:
		if offset > open.nodeStat.node.Size {
			return 0, errors.New("Seek: invalid offset")
		} else if offset < 0 {
			return 0, errors.New("Seek: invalid offset")
		} else if open.nodeRes != nil {
			open.nodeRes.Body.Close()
		}
		node.Header().Set("Range", fmt.Sprintf("bytes=%d-%d", offset, open.nodeStat.node.Size-1))
		var err error
		if open.nodeRes, err = node.Download(); err != nil {
			return 0, err
		}
		open.offset = offset
	case io.SeekEnd:
		offset = open.nodeStat.node.Size - offset
		if offset > open.nodeStat.node.Size {
			return 0, errors.New("Seek: invalid offset")
		} else if offset < 0 {
			return 0, errors.New("Seek: invalid offset")
		} else if open.nodeRes != nil {
			open.nodeRes.Body.Close()
		}
		node.Header().Set("Range", fmt.Sprintf("bytes=%d-%d", offset, open.nodeStat.node.Size-1))
		var err error
		if open.nodeRes, err = node.Download(); err != nil {
			return 0, err
		}
	case io.SeekCurrent:
		if _, err := io.CopyN(io.Discard, open.nodeRes.Body, offset); err != nil {
			return 0, err
		}
		offset += open.offset
	default:
		return 0, errors.New("Seek: invalid whence")
	}
	open.offset = offset
	return open.offset, nil
}

func (open *driveOpen) Read(p []byte) (int, error) {
	if open.nodeRes == nil {
		var err error
		if open.nodeRes, err = open.client.Files.Get(open.nodeStat.node.Id).AcknowledgeAbuse(true).Download(); err != nil {
			return 0, err
		}
	}
	n, err := open.nodeRes.Body.Read(p)
	open.offset += int64(n)
	return n, err
}

// resolve path and return File stream
func (gdrive *Gdrive) Open(path string) (Object, error) {
	fileNode, err := gdrive.GetNode(path)
	if err != nil {
		return nil, err
	}
	nodeTime, err := time.Parse(time.RFC3339, fileNode.CreatedTime)
	if err != nil {
		return nil, err
	}
	return &driveOpen{&driveStat{fileNode, nodeTime}, gdrive.driveService, nil, 0}, nil
}

func (gdrive *Gdrive) Delete(path string) error {
	fileNode, err := gdrive.GetNode(path)
	if err != nil {
		return err
	} else if err = gdrive.cacheDelete(path); err != nil {
		return err
	}
	return gdrive.driveService.Files.Delete(fileNode.Id).Do()
}

func (gdrive *Gdrive) IterateObjects(path string, iterator func(path string, obj Object) error) (err error) {
	var current *drive.File
	if current, err = gdrive.GetNode(path); err != nil {
		return err
	} else if current.MimeType != GoogleDriveMimeFolder {
		n := pathSplit(path)
		nodeTime, err := time.Parse(time.RFC3339, current.CreatedTime)
		if err != nil {
			return err
		}
		return iterator(n[len(n)-1][1], &driveOpen{&driveStat{current, nodeTime}, gdrive.driveService, nil, 0})
	}
	var recursiveCall func(path, folderID string) error
	recursiveCall = func(path, folderID string) error {
		files, err := gdrive.listNodes(folderID)
		if err != nil {
			return err
		}
		for _, k := range files {
			npath := fixPath(strings.Join([]string{path, k.Name}, "/"))
			if err = gdrive.cachePut(npath, k); err != nil {
				return err
			}
			if k.MimeType == GoogleDriveMimeFolder {
				if err = recursiveCall(npath, k.Id); err != nil {
					return err
				}
				continue
			}
			nodeTime, err := time.Parse(time.RFC3339, k.CreatedTime)
			if err != nil {
				return err
			} else if err := iterator(npath, &driveOpen{&driveStat{k, nodeTime}, gdrive.driveService, nil, 0}); err != nil {
				return err
			}
		}
		return nil
	}
	n := pathSplit(path)
	return recursiveCall(n[len(n)-1][1], current.Id)
}

func (gdrive *Gdrive) Save(path string, r io.Reader, size int64) (int64, error) {
	n := pathSplit(path)
	if stat, err := gdrive.Stat(path); err == nil {
		res, err := gdrive.driveService.Files.Update(stat.(*driveStat).node.Id, nil).Media(r).Do()
		if err != nil {
			return 0, err
		} else if err = gdrive.cachePut(n[len(n)-1][1], res); err != nil {
			return 0, err
		}
		return res.Size, nil
	}

	rootSolver := gdrive.rootDrive
	if checkMkdir(path) {
		var err error
		if rootSolver, err = gdrive.MkdirAll(n[len(n)-2][1]); err != nil {
			return 0, err
		}
	}

	var err error
	if rootSolver, err = gdrive.driveService.Files.Create(&drive.File{
		MimeType: "application/octet-stream",
		Name:     n[len(n)-1][0],
		Parents:  []string{rootSolver.Id},
	}).Fields("*").Media(r).Do(); err != nil {
		return 0, err
	} else if err = gdrive.cachePut(n[len(n)-1][1], rootSolver); err != nil {
		return 0, err
	}
	return rootSolver.Size, nil
}
