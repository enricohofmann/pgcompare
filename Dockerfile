###############################
# STEP 1 build the GO artefact #
################################
FROM golang:1.18-alpine AS builder

WORKDIR /src
COPY . .

# Setup Go Environments
ENV GO111MODULE=on
ENV GOPRIVATE=github.com/wecreateio
ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOARCH=amd64
ENV GOFLAGS=-mod=mod

# Load Go Dependencies
RUN go mod download

# Build the binary.
RUN go build -a -installsuffix cgo -ldflags="-w -s" -o bin/proxy proxy/portproxy.go

##############################
# STEP 2 build a small image #
##############################
FROM alpine

RUN addgroup -g 1000 -S proxy && \
    adduser -u 1000 -S proxy -G proxy

#Run Container as nonroot
USER 1000

WORKDIR /usr/local/bin

# Copy our static executable.
COPY --from=builder /src/bin/proxy /usr/local/bin/proxy
COPY ./run.sh /
#RUN sudo chown -R nonroot:nonroot /usr/local/bin/proxy
# Run the Server
CMD ["/run.sh"]