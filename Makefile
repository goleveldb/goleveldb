PACKAGE_NAME=github.com/goleveldb/goleveldb
MOCK_HOME=./internal/mock

# 生成writable_file的mock文件
gen_mock_file_writer:
	mockgen -destination ${MOCK_HOME}/file/writer.go -package file  ${PACKAGE_NAME}/file Writer

clean_mock:
	rm -rf ./internal/mock