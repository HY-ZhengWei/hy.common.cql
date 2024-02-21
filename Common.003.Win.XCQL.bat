

del /Q hy.common.xcql.jar
del /Q hy.common.xcql-sources.jar


call mvn clean package
cd .\target\classes


rd /s/q .\org\hy\common\xcql\junit

jar cvfm hy.common.xcql.jar META-INF/MANIFEST.MF META-INF org

copy hy.common.xcql.jar ..\..
del /q hy.common.xcql.jar
cd ..\..





cd .\src\main\java
xcopy /S ..\resources\* .
jar cvfm hy.common.xcql-sources.jar META-INF\MANIFEST.MF META-INF org 
copy hy.common.xcql-sources.jar ..\..\..
del /Q hy.common.xcql-sources.jar
rd /s/q META-INF
cd ..\..\..

pause