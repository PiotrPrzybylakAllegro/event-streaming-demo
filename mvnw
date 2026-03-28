#!/bin/sh
# Minimal Maven Wrapper Startup
BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
WRAPPER_JAR="$BASE_DIR/.mvn/wrapper/maven-wrapper.jar"
WRAPPER_LAUNCHER=org.apache.maven.wrapper.MavenWrapperMain
JAVA_CMD="${JAVA_HOME:+$JAVA_HOME/bin/}java"
MAVEN_PROJECTBASEDIR="${MAVEN_PROJECTBASEDIR:-$BASE_DIR}"

exec "$JAVA_CMD" \
  -Dmaven.multiModuleProjectDirectory="$MAVEN_PROJECTBASEDIR" \
  -cp "$WRAPPER_JAR" \
  $WRAPPER_LAUNCHER "$@"
