package main

// SensitiveFilter 基于 AC 自动机（Aho-Corasick）的本地敏感词过滤器
// 纯内存匹配，单次 O(len(text)) 扫描，不做网络调用，不阻塞主链路
type SensitiveFilter struct {
	root *acNode
}

type acNode struct {
	children map[rune]*acNode
	fail     *acNode
	isEnd    bool
	length   int // 命中敏感词的 rune 长度，用于确定替换范围
}

// defaultSensitiveWords 默认敏感词表（demo 用途的通用违规品类占位词，非真实黑名单）
// 生产环境应从配置/远程词库热加载
var defaultSensitiveWords = []string{
	"办假证", "加微信代刷单", "私彩博彩", "违禁枪支", "贩毒",
}

func NewSensitiveFilter(words []string) *SensitiveFilter {
	root := &acNode{children: make(map[rune]*acNode)}
	for _, w := range words {
		if w == "" {
			continue
		}
		node := root
		runes := []rune(w)
		for _, r := range runes {
			next, ok := node.children[r]
			if !ok {
				next = &acNode{children: make(map[rune]*acNode)}
				node.children[r] = next
			}
			node = next
		}
		node.isEnd = true
		node.length = len(runes)
	}
	root.buildFailLinks()
	return &SensitiveFilter{root: root}
}

// buildFailLinks BFS 构造 AC 自动机的失配指针
func (root *acNode) buildFailLinks() {
	queue := make([]*acNode, 0, len(root.children))
	for _, child := range root.children {
		child.fail = root
		queue = append(queue, child)
	}
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		for r, child := range node.children {
			fail := node.fail
			for fail != nil {
				if next, ok := fail.children[r]; ok {
					child.fail = next
					break
				}
				fail = fail.fail
			}
			if fail == nil {
				child.fail = root
			}
			queue = append(queue, child)
		}
	}
}

// Filter 扫描文本，命中的敏感词用等长 '*' 替换，未命中原样返回
func (f *SensitiveFilter) Filter(text string) string {
	runes := []rune(text)
	if len(runes) == 0 {
		return text
	}

	hit := make([]bool, len(runes))
	node := f.root
	hasHit := false

	for i, r := range runes {
		for node != f.root {
			if _, ok := node.children[r]; ok {
				break
			}
			node = node.fail
		}
		if next, ok := node.children[r]; ok {
			node = next
		} else {
			node = f.root
		}
		for n := node; n != f.root; n = n.fail {
			if n.isEnd {
				hasHit = true
				start := i - n.length + 1
				for j := start; j <= i; j++ {
					hit[j] = true
				}
			}
		}
	}

	if !hasHit {
		return text
	}
	for i, h := range hit {
		if h {
			runes[i] = '*'
		}
	}
	return string(runes)
}
