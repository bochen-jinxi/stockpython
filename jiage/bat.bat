@echo off
cd /d F:\stockpython\pyrun

:: 循环执行所有 py 脚本
for %%f in (*.py) do (
    echo 正在运行 %%f ...
    python "%%f"
)

echo 全部执行完成！
pause
