ARG GO_VERSION=1.26

# Go build stage
FROM golang:${GO_VERSION} AS build
ARG IMAGE_TAG
ENV CGO_ENABLED=0
COPY . /src/grackle
WORKDIR /src/grackle
RUN make clean && make grackle

# Prepare filesystem stage
FROM golang:${GO_VERSION} AS filesystem
RUN mkdir -p /grackle && \
    addgroup --gid 10001 grackle && \
    adduser --uid 10001 --gid 10001 --disabled-password --gecos "" grackle && \
    chown -R grackle:grackle /grackle

# Final stage
FROM gcr.io/distroless/static:noroot

COPY --from=build /src/grackle/cmd/grackle/grackle /usr/bin/grackle
COPY --from=filesystem --chown=10001:10001 /grackle /grackle
COPY --from=filesystem /etc/passwd /etc/passwd
COPY --from=filesystem /etc/group /etc/group

USER 10001
WORKDIR /
EXPOSE 8000
ENTRYPOINT [ "/usr/bin/grackle" ]
