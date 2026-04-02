#!/bin/bash
set -e

echo "Building nuclear WASM bindings..."

# 清理旧的构建产物
rm -rf pkg dist

# 构建 Web 目标
echo "Building for Web..."
wasm-pack build --target web --out-dir pkg/web --release

# 构建 Node.js 目标
echo "Building for Node.js..."
wasm-pack build --target nodejs --out-dir pkg/node --release

# 构建 Bundler 目标
echo "Building for Bundler..."
wasm-pack build --target bundler --out-dir pkg/bundler --release

# 创建 dist 目录结构
mkdir -p dist/web dist/node dist/types

# 复制 Web 构建产物
cp pkg/web/nuclear.js dist/web/
cp pkg/web/nuclear_bg.wasm dist/web/
cp pkg/web/nuclear.d.ts dist/web/

# 复制 Node.js 构建产物
cp pkg/node/nuclear.js dist/node/
cp pkg/node/nuclear_bg.wasm dist/node/
cp pkg/node/nuclear.d.ts dist/node/

# 生成统一的类型定义
cat > dist/types/index.d.ts << 'EOF'
export interface DatabaseOptions {
  nodeId?: string;
  basePath?: string;
}

export interface Record {
  meta: {
    id: string;
    createdAt: string;
    updatedAt: string;
  };
  data: any;
}

export interface QueryResult {
  records?: Record[];
  recordsAggregate?: {
    count: number;
  };
}

export class Database {
  static create(options?: DatabaseOptions): Promise<Database>;
  
  query(query: string): Promise<any>;
  mutation(mutation: string): Promise<any>;
  
  getSyncRequest(): Promise<any>;
  getChangesSince(clock: any): Promise<any[]>;
  applySyncResponse(response: any): Promise<void>;
  getClock(): Promise<any>;
  
  free(): void;
}

export function initSync(module: any): void;
export default function init(input?: any): Promise<void>;
EOF

echo "Build complete!"
echo ""
echo "Output structure:"
echo "  dist/web/     - Browser/Web WASM"
echo "  dist/node/    - Node.js WASM"
echo "  dist/types/   - TypeScript definitions"
