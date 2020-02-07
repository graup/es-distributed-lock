doc:
	go doc -all lock

clean:
	go clean

test:
	go test

coverage:
	go test -coverprofile=cover.out && go tool cover -html=cover.out