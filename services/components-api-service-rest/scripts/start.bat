@ECHO OFF

TITLE COMPONENT WEB SERVICE
SET APP_CLASS="org.talend.components.service.rest.Application"

SET CLASSPATH=.\config;${classpath.windows}
set THISDIR=%~dp0

java %JAVA_OPTS% -Xmx2048m -Dfile.encoding=UTF-8 -Dorg.ops4j.pax.url.mvn.localRepository="%THISDIR%\.m2" -Dorg.talend.component.jdbc.config.file="%THISDIR%\config\jdbc_config.json" -cp %CLASSPATH% %APP_CLASS%

