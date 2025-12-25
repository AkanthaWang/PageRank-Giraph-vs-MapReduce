#!/bin/bash
# Giraph PageRank 运行脚本 (Linux/Mac)

# 配置变量（根据你的环境修改）
USER_NAME=${USER}
INPUT_PATH="/user/${USER_NAME}/pagerank/input/web-Google-clean.txt"
OUTPUT_PATH="/user/${USER_NAME}/pagerank/output"
JAR_FILE="target/pagerank-comparison-1.0.jar"
WORKERS=4

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Giraph PageRank 运行脚本 ===${NC}"

# 检查 JAR 文件是否存在
if [ ! -f "$JAR_FILE" ]; then
    echo -e "${RED}错误: JAR 文件不存在: $JAR_FILE${NC}"
    echo -e "${YELLOW}请先运行: mvn clean package${NC}"
    exit 1
fi

# 检查输入路径是否存在
echo -e "${YELLOW}检查输入路径...${NC}"
hdfs dfs -test -e "$INPUT_PATH"
if [ $? -ne 0 ]; then
    echo -e "${RED}错误: 输入文件不存在: $INPUT_PATH${NC}"
    echo -e "${YELLOW}请先上传数据到 HDFS${NC}"
    exit 1
fi

# 删除旧输出（如果存在）
echo -e "${YELLOW}删除旧输出目录（如果存在）...${NC}"
hdfs dfs -rm -r -f "$OUTPUT_PATH" 2>/dev/null

# 显示配置信息
echo -e "${GREEN}配置信息:${NC}"
echo "  输入路径: $INPUT_PATH"
echo "  输出路径: $OUTPUT_PATH"
echo "  工作节点数: $WORKERS"
echo "  JAR 文件: $JAR_FILE"
echo ""

# 运行作业
echo -e "${GREEN}开始运行 PageRank 作业...${NC}"
echo ""

hadoop jar "$JAR_FILE" \
  edu.practice.pagerank.PageRankDriver \
  "$INPUT_PATH" \
  "$OUTPUT_PATH" \
  -Dgiraph.numComputeThreads=16 \
  -Dgiraph.numInputThreads=4 \
  -Dgiraph.numOutputThreads=4

# 检查运行结果
if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}=== 作业运行成功！ ===${NC}"
    echo ""
    echo -e "${YELLOW}查看结果:${NC}"
    echo "  hdfs dfs -ls $OUTPUT_PATH"
    echo ""
    echo -e "${YELLOW}查看前 10 个最高 PageRank 值:${NC}"
    echo "  hdfs dfs -cat $OUTPUT_PATH/part-m-* | sort -k2 -gr | head -10"
else
    echo ""
    echo -e "${RED}=== 作业运行失败！ ===${NC}"
    echo -e "${YELLOW}查看日志: yarn logs -applicationId <app_id>${NC}"
    exit 1
fi

