@echo off
cd /d F:\stockpython\pyrun

:: 循环执行所有 py 脚本
for %%f in (*.py) do (
    echo 正在运行 %%f ...
    python "%%f"
    
    :: 等待3分钟（180秒）
    echo 等待3分钟后执行下一个脚本...
    timeout /t 180 /nobreak
)

echo 全部执行完成！
pause