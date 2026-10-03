# The scraper's image for Unikraft Cloud. Its registry caps an account's
# images at 1 GiB, and Playwright's image is 2.6 GB, so this is only what the
# scraper runs — node, Chromium's headless shell, the libraries they link, a
# shell and fonts — taken from Playwright's image, on nothing else (~580 MB).
#
# Build context: a directory holding
#   rn8                      static rn8 (x86_64-unknown-linux-musl)
#   sdk/                     the TS SDK's src/ and examples/sandbox/, with
#                            node_modules installed beside them: only
#                            cron-parser, eventsource and tsx at the top level
#
# See ../README.md for the steps.
FROM mcr.microsoft.com/playwright:v1.56.1-noble AS full
ARG SHELL_DIR=/ms-playwright/chromium_headless_shell-1194
# The last link: Unikraft keeps the files, not the image's ENV, so the browser
# is also where Playwright looks when PLAYWRIGHT_BROWSERS_PATH is unset.
RUN mkdir -p /out && \
    for f in $(ldd $SHELL_DIR/chrome-linux/headless_shell /usr/bin/node /usr/bin/env /usr/bin/dash \
                 /usr/lib/x86_64-linux-gnu/libsoftokn3.so /usr/lib/x86_64-linux-gnu/libfreeblpriv3.so \
                 /usr/lib/x86_64-linux-gnu/libnssckbi.so 2>/dev/null \
               | awk '/=>/ {print $3}' | sed 's#^/lib/#/usr/lib/#' | sort -u) \
             /usr/lib/x86_64-linux-gnu/libsoftokn3.so /usr/lib/x86_64-linux-gnu/libfreeblpriv3.so \
             /usr/lib/x86_64-linux-gnu/libnssckbi.so \
             /usr/lib64/ld-linux-x86-64.so.2 /usr/lib/x86_64-linux-gnu/ld-linux-x86-64.so.2; do \
      [ -e "$f" ] || continue; \
      cp -a --parents "$f" /out; cp -a --parents "$(readlink -f "$f")" /out; \
    done && \
    cp -a --parents /usr/bin/node /usr/bin/env /usr/bin/dash /etc/passwd /etc/group /etc/nsswitch.conf \
      /etc/fonts /usr/share/fonts /var/cache/fontconfig $SHELL_DIR /out && \
    ln -s dash /out/usr/bin/sh && ln -s usr/bin /out/bin && ln -s usr/lib /out/lib && \
    ln -s usr/lib64 /out/lib64 && mkdir -p /out/tmp /out/dev/shm /out/proc /out/sys && chmod 1777 /out/tmp && \
    mkdir -p /out/.cache && ln -s /ms-playwright /out/.cache/ms-playwright

FROM scratch
COPY --from=full /out/ /
ENV PATH=/usr/local/bin:/usr/bin PLAYWRIGHT_BROWSERS_PATH=/ms-playwright HOME=/tmp
COPY rn8 /usr/local/bin/rn8
COPY sdk /sdk
WORKDIR /sdk
