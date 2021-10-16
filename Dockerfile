# syntax=docker/dockerfile:1
# builder image
FROM golang:1.17 as builder
RUN mkdir /build
ADD *.go /build/
ADD go.mod /build/
ADD go.sum /build/
WORKDIR /build
RUN CGO_ENABLED=0 GOOS=linux go build -a -o main .


# generate clean, final image for end users
FROM alpine:3.11.3
COPY --from=builder /build/main .

EXPOSE 80 443
# executable
ENTRYPOINT [ "./main" ]
# arguments that can be overridden
#CMD [ "3", "300" ]