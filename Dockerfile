# Runtime image for the kapso-whatsapp-bridge daemon.
#
# GoReleaser cross-compiles the static (CGO_ENABLED=0) binary for the target
# architecture and places it in the build context; this image just wraps it.
# distroless/static:nonroot has no shell or package manager and runs as an
# unprivileged user (uid 65532).
FROM gcr.io/distroless/static:nonroot

COPY kapso-whatsapp-bridge /usr/bin/kapso-whatsapp-bridge

ENTRYPOINT ["/usr/bin/kapso-whatsapp-bridge"]
