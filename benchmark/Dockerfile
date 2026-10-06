# Generic image for benchmarking a Flink distribution (stock or the Map-Filter fusion
# prototype). The build context is expected to contain:
#   flink/           - a full Flink distribution (bin/, lib/, conf/, ...)
#   benchmark.jar    - every benchmark job class under
#                      benchmark/job/org/apache/flink/demo/ (entrypoint.sh is told which
#                      one to run via the JOB_CLASS env var)
#   entrypoint.sh    - starts a local cluster, runs the job N times, prints results
#
# See benchmark/run_benchmark.sh, which stages this context for both variants.
FROM eclipse-temurin:17-jre

ENV FLINK_HOME=/opt/flink
ENV PATH="${FLINK_HOME}/bin:${PATH}"

COPY flink/ ${FLINK_HOME}/
COPY benchmark.jar ${FLINK_HOME}/benchmark.jar
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8081

ENTRYPOINT ["/entrypoint.sh"]
