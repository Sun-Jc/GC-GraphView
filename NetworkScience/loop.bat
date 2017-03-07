@echo off
:start
echo %1
bin\x64\debug\networkscience.exe %1
IF %1=="node" (goto start)
IF %1=="edge" (goto start)