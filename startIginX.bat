@echo off
echo ````````````````````````
echo Starting IginX
echo ````````````````````````


set PATH="%JAVA_HOME%\bin\";%PATH%
set "FULL_VERSION="
set "MAJOR_VERSION="
set "MINOR_VERSION="


for /f tokens^=2-5^ delims^=.-_+^" %%j in ('java -fullversion 2^>^&1') do (
	set "FULL_VERSION=%%j-%%k-%%l-%%m"
	IF "%%j" == "1" (
	    set "MAJOR_VERSION=%%k"
	    set "MINOR_VERSION=%%l"
	) else (
	    set "MAJOR_VERSION=%%j"
	    set "MINOR_VERSION=%%k"
	)
)

set JAVA_VERSION=%MAJOR_VERSION%

@REM we do not check jdk that version less than 1.6 because they are too stale...
IF "%JAVA_VERSION%" == "6" (
		echo IginX only supports jdk >= 8, please check your java version.
		goto finally
)
IF "%JAVA_VERSION%" == "7" (
		echo IginX only supports jdk >= 8, please check your java version.
		goto finally
)

if "%OS%" == "Windows_NT" setlocal

pushd %~dp0
if NOT DEFINED IGINX_HOME set IGINX_HOME=%cd%
popd

set IGINX_CONF=%IGINX_HOME%\conf

@setlocal ENABLEDELAYEDEXPANSION ENABLEEXTENSIONS
set is_conf_path=false
for %%i in (%*) do (
	IF "%%i" == "-c" (
		set is_conf_path=true
	) ELSE IF "!is_conf_path!" == "true" (
		set is_conf_path=false
		set IGINX_CONF=%%i
	) ELSE (
		set CONF_PARAMS=!CONF_PARAMS! %%i
	)
)

if NOT DEFINED MAIN_CLASS set MAIN_CLASS=cn.edu.tsinghua.iginx.Iginx
if NOT DEFINED JAVA_HOME goto :err

@REM -----------------------------------------------------------------------------
@REM JVM Opts we'll use in legacy run or installation
set JAVA_OPTS=-ea^
 -DIGINX_HOME="%IGINX_HOME%"^
 -DIGINX_CONF="%IGINX_CONF%"

set MAX_HEAP_SIZE=3072M
set HEAP_NEWSIZE=3072M
set HEAP_OPTS=-Xmx%MAX_HEAP_SIZE% -Xms%HEAP_NEWSIZE% -Xloggc:"%IGINX_HOME%\gc.log" -XX:+PrintGCDateStamps -XX:+PrintGCDetails

@REM ***** CLASSPATH library setting *****
@REM Ensure that any user defined CLASSPATH variables are not used on startup
set CLASSPATH=%CLASSPATH%;"%IGINX_HOME%\core\target\iginx-core-0.1.0-SNAPSHOT.jar"
set CLASSPATH=%CLASSPATH%;"%IGINX_HOME%\iotdb\target\iotdb-0.1.0-SNAPSHOT.jar"
set CLASSPATH=%CLASSPATH%;"%IGINX_HOME%\influxdb\target\influxdb-0.1.0-SNAPSHOT.jar"
goto okClasspath

:append
set CLASSPATH=%CLASSPATH%;%1

goto :eof

@REM -----------------------------------------------------------------------------
:okClasspath

echo CLASSPATH: %CLASSPATH%

"%JAVA_HOME%\bin\java" %JAVA_OPTS% %HEAP_OPTS% -cp %CLASSPATH% %MAIN_CLASS%
goto finally

:err
echo JAVA_HOME environment variable must be set!
pause


@REM -----------------------------------------------------------------------------
:finally

pause

ENDLOCAL
