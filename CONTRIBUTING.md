# CONTRIBUTING 约定

作为共同协作的项目，风格的统一非常重要，我们将共同遵守下方的规则。

## 1. 代码规范

### 1.1 请遵守谷歌golang开发规范
https://github.com/golang/go/wiki/CodeReviewComments

### 1.2 请确保代码通过了`golangci-lint`扫描
https://golangci-lint.run/

### 1.3 推荐使用`wsl`工具对换行进行检测
https://github.com/bombsimon/wsl

建议在使用`wsl`前，先阅读`wsl/doc/rules.md`中的实例。

## 2. git 相关

### 2.1 使用`git-flow`
https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow

一句话概括就是：
开发时从`develop`分支切出`feature`，开发完成后提`MR`合并到`develop`

### 2.2 `commit message` 需要遵守规范
https://chris.beams.io/posts/git-commit/

### 2.3 `commit` 变更不要太多
一次commit中，代码的变更量最好控制在`200` ~ `400`行之间。

### 2.4 `CR` && `MR` ！！！
CR指的是：`Code Review`，请在开发过程中进行`CR`，一次`CR`尽量不要超过500行。

请通过`MR`进行合并，不要直接操作分支。

## 3. 单元测试

### 3.1 单元测试覆盖率需要达到70%
实际上， 70% 的覆盖率非常低， 如果你的覆盖率低于 70 % ， 你也可以发起`MR`，我们共同讨论。

### 3.2 Mock
好的代码设计可以减少mock的使用，请尽量避免mock。

#### 3.2.1 mock 时使用 go:generate
mock时请使用`go:generate`进行mock文件的生成，生成的文件存放在`internal/mock`