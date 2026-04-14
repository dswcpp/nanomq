# 在 Ubuntu 上交叉打包 Windows 64 位 NanoMQ 安装包

本文用于记录当前仓库在 Ubuntu 环境下交叉打包 Windows 64 位 NanoMQ 安装包的实际流程。

本文默认以下环境已经准备完成，因此不再重复介绍环境安装步骤：

- `llvm-mingw` 已安装到 `/opt/llvm-mingw`
- `NSIS` 已安装，`makensis` 可直接使用
- 当前仓库已包含 Windows 交叉编译所需的修复

本文基于当前仓库版本 `0.24.15` 验证通过，最终可产出两类文件：

- Windows 便携压缩包：`nanomq-nng-v0.24.15_amd64.zip`
- Windows 安装包：`nanomq-nng-v0.24.15_amd64.exe`

## 1. 进入仓库

```bash
cd /home/tery/project/weld/nanomq
```

## 2. 配置 Windows x64 构建目录

使用当前已经验证通过的参数重新生成 `build-win64`：

```bash
cmake -S . -B build-win64 \
  -DCMAKE_SYSTEM_NAME=Windows \
  -DCMAKE_C_COMPILER=/opt/llvm-mingw/bin/x86_64-w64-mingw32-clang \
  -DCMAKE_CXX_COMPILER=/opt/llvm-mingw/bin/x86_64-w64-mingw32-clang++ \
  -DNNG_ENABLE_TLS=OFF \
  -DBUILD_NANOMQ_CLI=ON \
  -DBUILD_CLIENT=ON \
  -DNANOMQ_TESTS=OFF \
  -DNNG_ENABLE_SQLITE=OFF
```

说明：

- `CMAKE_SYSTEM_NAME=Windows`：启用 Windows 目标平台
- `llvm-mingw`：使用 UCRT 兼容的交叉编译器
- `NNG_ENABLE_TLS=OFF`：当前已验证流程不包含 Windows 侧 TLS 依赖
- `NANOMQ_TESTS=OFF`：打包时不编译单元测试
- `NNG_ENABLE_SQLITE=OFF`：当前已验证流程不启用 SQLite 依赖

如果需要从头重新打包，可以先删除旧目录：

```bash
rm -rf build-win64 dist-win64 dist-win64-nsis
```

## 3. 编译 Windows x64 可执行文件

```bash
cmake --build build-win64 --parallel 8
```

编译成功后，核心产物位于：

- `build-win64/nanomq/nanomq.exe`
- `build-win64/nanomq_cli/nanomq_cli.exe`

## 4. 生成 Windows ZIP 包

```bash
cd /home/tery/project/weld/nanomq/build-win64
cpack -G ZIP -B /home/tery/project/weld/nanomq/dist-win64
```

生成结果：

- `/home/tery/project/weld/nanomq/dist-win64/nanomq-nng-v0.24.15_amd64.zip`

包内已验证包含以下关键文件：

- `bin/nanomq.exe`
- `bin/nanomq_cli.exe`
- `bin/libwinpthread-1.dll`
- `config/nanomq.conf`
- `config/nanomq_weld_taos.conf`
- `config/weld_tdengine_schema.sql`
- `config/weld_tdengine_schema_reset.sql`

## 5. 生成 Windows EXE 安装包

仍然在 `build-win64` 目录下执行：

```bash
cpack -G NSIS64 -B /home/tery/project/weld/nanomq/dist-win64-nsis
```

生成结果：

- `/home/tery/project/weld/nanomq/dist-win64-nsis/nanomq-nng-v0.24.15_amd64.exe`

## 6. 检查产物

查看 ZIP 包：

```bash
ls -lh /home/tery/project/weld/nanomq/dist-win64/nanomq-nng-v0.24.15_amd64.zip
```

查看 EXE 安装包：

```bash
ls -lh /home/tery/project/weld/nanomq/dist-win64-nsis/nanomq-nng-v0.24.15_amd64.exe
```

如果只想确认核心 exe 是否已生成，可执行：

```bash
find /home/tery/project/weld/nanomq/build-win64 -maxdepth 3 \
  \( -name 'nanomq.exe' -o -name 'nanomq_cli.exe' \) -print
```

## 7. 本流程对应的当前修复点

本流程依赖当前仓库内已经存在的以下修复：

- `nng/src/platform/windows/win_impl.h`
- `nng/src/platform/windows/win_thread.c`
- `nanomq/CMakeLists.txt`

如果切换到更旧的分支或提交后重新打包，出现与本文不一致的构建错误，优先检查这些文件中的 Windows 交叉编译兼容改动是否仍然存在。

## 8. 常见问题

### 8.1 `Modern Windows API support is missing`

原因通常是：

- 没有使用 `llvm-mingw` 的 UCRT 工具链
- 误用了系统默认的 `x86_64-w64-mingw32-gcc/g++`

确认当前命令使用的是：

- `/opt/llvm-mingw/bin/x86_64-w64-mingw32-clang`
- `/opt/llvm-mingw/bin/x86_64-w64-mingw32-clang++`

### 8.2 `Cannot find NSIS compiler makensis`

说明 `NSIS` 没有安装，或者 `makensis` 不在 `PATH` 中。

本文默认该环境已经准备完成；若重新换机器，需要先确保：

```bash
makensis -VERSION
```

可正常输出版本号。

### 8.3 `dlfcn.h file not found`

这通常表示 Windows 打包时仍然错误地编译了 POSIX 侧测试目标。

优先确认：

- `nanomq/CMakeLists.txt` 中 `tests` 子目录不是无条件加入
- 重新执行一次第 2 步的 `cmake -S . -B build-win64 ...`

### 8.4 `unable to find library -lsqlite3`

这通常表示当前 Windows 打包流程仍在无条件链接 `sqlite3`。

优先确认：

- 第 2 步里已显式传入 `-DNNG_ENABLE_SQLITE=OFF`
- `nanomq/CMakeLists.txt` 中 `sqlite3` 链接不是无条件执行
- 重新执行一次第 2 步的 `cmake -S . -B build-win64 ...`

## 9. 建议的发布顺序

建议每次按以下顺序操作：

1. 重新执行第 2 步配置
2. 执行第 3 步编译
3. 先生成 ZIP 包验证内容
4. 再生成 EXE 安装包
5. 最后对两个产物执行 `sha256sum`

示例：

```bash
sha256sum /home/tery/project/weld/nanomq/dist-win64/nanomq-nng-v0.24.15_amd64.zip
sha256sum /home/tery/project/weld/nanomq/dist-win64-nsis/nanomq-nng-v0.24.15_amd64.exe
```
