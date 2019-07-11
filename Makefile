 # Go parameters
GOCMD=go
GOTEST=$(GOCMD) test

test:
	$(GOTEST) -race -v ./...
