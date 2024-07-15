FROM golang:1.21

WORKDIR /go/project/priceProvider

ADD go.mod go.sum main.go ./
ADD internal ./internal

EXPOSE 9092

CMD ["go", "run", "main.go"]
