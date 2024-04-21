FROM golang:1.19-alpine as builder

RUN apk add --no-cache gcc musl-dev
RUN apk add --no-cache make

COPY . /app

WORKDIR /app

RUN make build 


FROM alpine

COPY --from=builder /app/build/cosmos-pruner /usr/bin/cosmprund

ENTRYPOINT [ "/usr/bin/cosmprund" ]
