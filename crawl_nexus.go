package main

import (
	"flag"
	"fmt"
	"os"
	"log"
	"time"
	"net/http"
	"path/filepath"
	"sync"
	"errors"
	"crypto/md5"
	"crypto/sha1"
	"io/ioutil"
	"encoding/hex"
)

//Options:
//-h, --help            show this help message and exit
//--maven-repository=MAVEN_REPOSITORY
//Folder containing the exploded maven-repository
//--repository-name=REPOSITORY_NAME
//Repository name or release group to test. Defaults to
///ga/
//--jars-only           Check for .jar localFiles only
//--verbose             Print results for each file/folder
//--json                Dump missing artifacts to a .json file
//--test                Don't actually HTTP GET artifacts
//--md5Sum                 Verify md5Sum checksums
//--sha1Sum                Verify sha1Sum checksums

var mavenRepo = flag.String("maven-repository", "", "path to directory containing the exploded maven-repository. Required")
var mavenRepoName = flag.String("repository-name", "ga", "Repository name or release group to test. Optional")
var nexusRoot = flag.String("nexus-root", "https://maven.repository.redhat.com", "Nexus base URL. Optional")
var jarsOnly = flag.Bool("jars-only", false, "Check for .jar localFiles only. Optional")
var json = flag.Bool("json", false, "Dump missing artifacts to a .json file. Optional")
var test = flag.Bool("test", false, "Don't actually HTTP GET artifacts. Optional")
var md5Sum = flag.Bool("md5Sum", false, "Verify md5Sum checksums. Optional")
var sha1Sum = flag.Bool("sha1Sum", false, "Verify sha1Sum checksums. Optional")
var verbose = flag.Bool("verbose", false, "Print results for each file/folder. Optional")
var threads = flag.Int("threads", 20, "The number of parallel threads to use to connect to repository. Optional")

var repo Repository

type Repository struct {
	repoName       string
	basePathLocal  string
	basePathRemote string
	lostDirs       []string
	lostFiles      []string
}

type Result struct {
	path string
	code int
	status  string
	err  error
	isDir bool
}

type LocalArtifact struct {
	path string
	md5 string
	sha1 string
	isDir bool
}

func init() {
	flag.Parse()
	if *mavenRepo != "" {
		repo = Repository{
			basePathLocal:  *mavenRepo,
			basePathRemote: *nexusRoot,
			lostDirs:       []string{},
			lostFiles:      []string{},
		}

	} else {
		fmt.Println("Required arg is missed...")
		fmt.Println("Usage:")
		flag.PrintDefaults()
		os.Exit(3)
	}
}

func main() {
	err := scan()
	if err != nil {
		log.Printf("Scan error: %v", err.Error())
	}
	log.Printf("Repo: %v", repo)
}

func scanLocalPath(done <-chan struct{}, rootPath string) (<-chan LocalArtifact, <-chan error) {
	artifacts := make(chan LocalArtifact)
	errs := make(chan error, 1)
	go func () {
		defer close(artifacts)
		absoluteLocalPath := *mavenRepo + rootPath
		errs <- filepath.Walk(absoluteLocalPath, func(path string, f os.FileInfo, err error) error {
			relativePath, relPathErr := filepath.Rel(*mavenRepo, path)
			if relPathErr != nil {
				return relPathErr
			}

			var fileMd5 [16]byte
			var fileSha1 [20]byte

			if !f.IsDir() {
				file, err := ioutil.ReadFile(path)
				if err != nil {
					return err
				}
				fileMd5 = md5.Sum(file)
				fileSha1 = sha1.Sum(file)
			}
			select {
				case artifacts <- LocalArtifact {
					relativePath,
					hex.EncodeToString(fileMd5[:]),
					hex.EncodeToString(fileSha1[:]),
					f.IsDir(),
				}:
				case <- done:
					return errors.New("Scan cancelled ...")
			}
			return nil
		})
	}()
	return artifacts, errs
}

func scanRemotePath(done <-chan struct{}, artifacts <-chan LocalArtifact, res chan<- Result) {
	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    30 * time.Second,
		DisableCompression: true,
	}
	client := &http.Client{
		Transport: tr,
	}
	var resp *http.Response
	var err error
	for artifact := range artifacts {
		relPath := artifact.path
		url := repo.basePathRemote + "/" + *mavenRepoName +  "/" + relPath

		if  !*test { //TODO: delete negation
			resp, err = client.Head(url)
		} else {
			resp, err = client.Get(url)
		}
		select {
		case res <- Result {
			url,
			resp.StatusCode,
			resp.Status,
			err,
			artifact.isDir,
			}:
		case <- done:
			return
		}
	}
}

func scan() error {
	done := make(chan struct{})
	defer close(done)

	artifacts, errs := scanLocalPath(done, "")
	res := make(chan Result)
	var wg sync.WaitGroup
	wg.Add(*threads)
	for i := 0; i < *threads; i++ {
		go func() {
			scanRemotePath(done, artifacts, res)
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(res)
	}()

	for r := range res {
		if r.err != nil {
			return r.err
		}
		dirsAcceptable := []int{200, 301, 302}
		fileAcceptable := 200
		var msg string
		msg = fmt.Sprintf("artifact: %v status: %v", r.path, r.status)
		if r.isDir {
			for _,  code := range dirsAcceptable {
				if code == r.code {
					break
				} else {
					repo.lostDirs = append(repo.lostDirs, r.path)
					msg = fmt.Sprintf("Dir %v is lost. Code: %v vs %v", r.path, r.code, code)
				}
			}
		} else {
			if fileAcceptable != r.code {
				repo.lostFiles = append(repo.lostFiles, r.path)
				msg = fmt.Sprintf("File %v is lost. Code: %v vs %v", r.path, r.code, fileAcceptable)
			}
		}
		
		if *verbose {
			log.Println(msg)
		}
	}

	if err := <- errs; err != nil {
		return err
	}
	return nil
}
