# Build the spark-sql-perf jar from the local source tree (includes com.wgq.Tpcds).
# Final image only contains the jar at /out/, intended to be COPYed into a Spark image
# (see ../app/Dockerfile for the runtime composition).
#
# Build:
#   docker build -t spark-sql-perf-builder:local .
#   docker create --name x spark-sql-perf-builder:local && \
#     docker cp x:/out/. ./target/docker-out/ && docker rm x
# Or use it as a multi-stage source:
#   FROM spark-sql-perf-builder:local AS sslperf
#   COPY --from=sslperf /out/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar /opt/spark/jars/

# syntax=docker/dockerfile:1.6

ARG SBT_VERSION=1.10.5

##############################
# Stage 1: sbt package        #
##############################
FROM eclipse-temurin:11-jdk-jammy AS builder

ARG SBT_VERSION

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" \
      | tar -xz -C /opt \
 && ln -s /opt/sbt/bin/sbt /usr/local/bin/sbt

WORKDIR /src

# Copy build descriptors first so dependency resolution layer is cached
# until build.sbt / project changes.
COPY build.sbt version.sbt ./
COPY project/ project/

RUN --mount=type=cache,target=/root/.sbt \
    --mount=type=cache,target=/root/.cache/coursier \
    --mount=type=cache,target=/root/.ivy2 \
    sbt -no-colors -Dsbt.log.noformat=true update

# Now copy the sources and build.
COPY src/ src/

RUN --mount=type=cache,target=/root/.sbt \
    --mount=type=cache,target=/root/.cache/coursier \
    --mount=type=cache,target=/root/.ivy2 \
    sbt -no-colors -Dsbt.log.noformat=true package \
 && mkdir -p /out \
 && cp target/scala-2.12/spark-sql-perf_*.jar /out/


