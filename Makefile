.PHONY : all deps test doc clean coverage

all: deps test

deps:
	go get ./...

test:
	go test -race
	
doc:
	go doc -all lock

clean:
	go clean

coverage:
	go test -coverprofile=cover.out && go tool cover -html=cover.out