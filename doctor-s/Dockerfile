FROM golang:1.22-alpine AS builder

WORKDIR /app

# Download dependencies first (better layer caching)
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build a statically linked binary
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /doctor-s ./cmd/doctor-s/

# Final image
FROM alpine:3.19

WORKDIR /app

# Binary
COPY --from=builder /doctor-s .

# Migration files must be at ./migrations/ relative to the working directory
# because main.go uses file://migrations
COPY migrations/ ./migrations/

EXPOSE 8081

CMD ["/app/doctor-s"]
