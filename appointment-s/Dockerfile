FROM golang:1.22-alpine AS builder

WORKDIR /workspace

# Copy both modules so the local replace directive resolves correctly
COPY doctor-s/go.mod doctor-s/go.sum ./doctor-s/
COPY appointment-s/go.mod appointment-s/go.sum ./appointment-s/

# Download deps for appointment-s (resolve replace => ../doctor-s = ./doctor-s)
WORKDIR /workspace/appointment-s
RUN go mod download

# Copy full source of both services
WORKDIR /workspace
COPY doctor-s/ ./doctor-s/
COPY appointment-s/ ./appointment-s/

# Build
WORKDIR /workspace/appointment-s
RUN CGO_ENABLED=0 GOOS=linux go build -o /appointment-s ./cmd/appointment-s/

# Final image
FROM alpine:3.19

WORKDIR /app

COPY --from=builder /appointment-s .
COPY --from=builder /workspace/appointment-s/migrations/ ./migrations/

EXPOSE 8082

CMD ["/app/appointment-s"]
