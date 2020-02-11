test:
	go test
	
doc:
	go doc -all lock

clean:
	go clean

coverage:
	go test -coverprofile=cover.out && go tool cover -html=cover.out