# Appointment Service (gRPC Migration) - Medical Scheduling Platform

## 1. Project Overview and Purpose
The Medical Scheduling Platform is a distributed system consisting of two microservices designed to manage doctors and patient appointments. This repository contains the Appointment Service, which has been  migrated from HTTP/REST to **gRPC** for all inter-service and client-to-server communication.

The purpose of this migration is to enforce strict service contracts using Protocol Buffers, accelerate data exchange via binary formatting (HTTP/2), and demonstrate the implementation of Clean Architecture alongside modern RPC frameworks in Go.

## 2. Service Responsibilities and Data Ownership
The system is divided into two bounded contexts:
* **Appointment Service (This service):** Owns the `Appointment` domain model. It is responsible for creating patient appointments, storing appointment history, and managing safe status transitions (`new` -> `in_progress` -> `done`).
* **Doctor Service:** Owns the `Doctor` domain model. It is responsible for managing and storing doctor profiles.
  *Inter-service Relationship:* The Appointment Service does not store doctor data locally. Instead, it calls the Doctor Service via gRPC to verify a doctor's existence before allowing a new appointment to be created.

## 3. Installation and Stub Regeneration
To modify the `.proto` contracts and regenerate the Go stubs, you will need the Protocol Buffers compiler and the respective Go plugins.

**Step 1: Install `protoc`**
Download the pre-compiled binary for your operating system from the [official GitHub releases page](https://github.com/protocolbuffers/protobuf/releases), extract it, and add the `bin` folder to your system's `PATH`.

**Step 2: Install Go gRPC plugins**
Run the following commands in your terminal:
```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

**Step 3: Regenerate Stubs**
If you modify the `proto/appointment.proto` file, regenerate the `.pb.go` stubs by running this command from the root directory of the Appointment Service:
`protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/appointment.proto`

## 4. Local Setup and Startup Instructions
Both services require a running MongoDB instance (default port: `localhost:27017`). The `appointment_db` and its corresponding collections will be created automatically.

**Startup Order:**
1. **First**, start the Doctor Service (Port `8081`) so the Appointment Service can successfully establish a client connection (`ClientConn`) upon startup.
2. **Second**, start the Appointment Service (Port `8082`).

**Step-by-step commands (in separate terminal windows):**

*Terminal 1 (Doctor Service):*
```bash
cd doctor-service
go mod tidy
go run ./cmd/doctor-s/main.go
```

*Terminal 2 (Appointment Service):*
```bash
cd appointment-service
go mod tidy
go run ./cmd/appointment-s/main.go
```

## 5. Proto Contract Description
The Appointment Service exposes the following RPCs to manage appointments and enforce business logic:

| RPC | Request | Response | Enforced Business Rule |
| :--- | :--- | :--- | :--- |
| **`CreateAppointment`** | `CreateAppointmentRequest` | `AppointmentResponse` | Requires `title` and `doctor_id`. Performs a gRPC call to the Doctor Service to verify the doctor exists. Returns `codes.InvalidArgument` if fields are missing, or `codes.FailedPrecondition` if the doctor is not found. Newly created appointments automatically receive the status `new`. |
| **`GetAppointment`** | `GetAppointmentRequest` | `AppointmentResponse` | Retrieves an appointment by its ID. Returns a `codes.NotFound` gRPC status if the ID does not exist. |
| **`ListAppointments`** | `ListAppointmentsRequest` | `ListAppointmentsResponse` | Returns an array of all appointments in the system. If no appointments exist, it returns an empty array (not nil). |
| **`UpdateAppointmentStatus`**| `UpdateStatusRequest` | `AppointmentResponse` | Updates the status of an appointment. Valid statuses are `new`, `in_progress`, and `done`. Transitioning from `done` back to `new` is strictly forbidden and will return `codes.InvalidArgument`. |

## 6. Inter-Service Communication
This architecture implements synchronous gRPC communication between the services:
* **When:** The Appointment Service calls the Doctor Service immediately during the execution of `CreateAppointment` (before saving any data to its own database).
* **How:** A `DoctorClient` interface is injected into the Use Case layer of the Appointment Service. This interface wraps the generated gRPC client stub, which invokes the `GetDoctor` RPC over the network.
* **Error Propagation:** If the Doctor Service returns a `codes.NotFound` gRPC status (doctor doesn't exist), the client stub maps this to a local domain error (`ErrDoctorNotExists`). The Appointment Service's delivery layer intercepts this domain error and returns a **`codes.FailedPrecondition`** status to the end-user.

## 7. Failure Scenario & Resilience Patterns
**What happens if the Doctor Service is unavailable?**
If the Doctor Service crashes or is completely unreachable, the gRPC call initiated by the Appointment Service will fail with a network connection error. The client stub catches this and throws a domain error (`ErrDoctorServiceDown`). The Appointment Service does not crash; instead, its transport layer intercepts this error and gracefully returns a **`codes.Unavailable`** gRPC status to the caller, indicating that the upstream dependency is down.

**Production Resilience Patterns:**
To improve stability in a production environment, the following advanced resilience patterns should be applied:
1. **Timeouts:** Applying `context.WithTimeout` to client requests ensures that the Appointment Service does not hang indefinitely waiting for an unresponsive Doctor Service.
2. **Retries:** Using gRPC interceptors to automatically retry requests (e.g., up to 3 times) can mitigate transient network glitches.
3. **Circuit Breakers:** Implementing a circuit breaker on the client side instantly fails requests to the Doctor Service if it consistently returns errors. This prevents resource exhaustion on the Appointment Service and gives the Doctor Service time to recover.

## 8. Trade-off Discussion: REST vs. gRPC
Choosing between REST and gRPC involves several concrete engineering trade-offs:

1. **Payload Format and Parsing:**
    * *REST* heavily relies on JSON. It is human-readable and universally supported but is text-heavy and slower to serialize/deserialize.
    * *gRPC* uses Protocol Buffers (binary). While unreadable without the corresponding `.proto` file, it is exceptionally compact and provides incredibly fast serialization/deserialization, saving significant CPU cycles.
2. **Contract Enforcement:**
    * In *REST*, API documentation (like OpenAPI/Swagger) is often maintained separately from the code and can easily drift out of sync with the actual implementation.
    * In *gRPC*, the `.proto` file acts as the single source of truth. Server and client stubs are generated directly from this contract, guaranteeing strict type safety at compile time and preventing structural mismatches.
3. **Underlying Protocol and Streaming:**
    * *REST* typically operates over HTTP/1.1, functioning on a strict unary Request-Response model and suffering from head-of-line blocking.
    * *gRPC* requires HTTP/2, enabling request multiplexing over a single TCP connection and providing native support for bi-directional streaming.

**When to choose which?**
REST remains the best choice for public-facing APIs or browser-based frontends due to its simplicity and native JSON support. However, **gRPC** is the superior choice for internal, backend-to-backend communication (such as the interaction between our Appointment and Doctor services), where strict contract enforcement, low latency, and high throughput are essential.