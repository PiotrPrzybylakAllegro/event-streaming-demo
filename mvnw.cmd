@ECHO OFF
set BASE_DIR=%~dp0
set WRAPPER_JAR=%BASE_DIR%\.mvn\wrapper\maven-wrapper.jar
set WRAPPER_LAUNCHER=org.apache.maven.wrapper.MavenWrapperMain
java -classpath %WRAPPER_JAR% %WRAPPER_LAUNCHER% %*
