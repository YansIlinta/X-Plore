package main

import (
	"encoding/json"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"
)

// WordEntry 房间敏感词条目：word 为词或正则表达式，mode 决定裁决。
type WordEntry struct {
	Word    string `json:"word"`
	Mode    string `json:"mode"` // "block"=屏蔽（打码），"flag"=放行但标记
	IsRegex bool   `json:"is_regex,omitempty"`
}

// WordBank 房间级敏感词库（可配置词库 + 屏蔽/送审双裁决）。
// 与 server/filter.go 的全局 AC 过滤分层：全局 AC 无条件打码（默认词表），
// 房间词库可增删、可正则、可只标记不屏蔽。Go regexp 基于 RE2，线性时间，
// 不存在灾难性回溯（ReDoS）风险。
type WordBank struct {
	mu    sync.RWMutex
	rooms map[string]map[string]WordEntry // roomID -> word -> entry
	regex map[string]*regexp.Regexp       // roomID|word -> 编译后的正则缓存
	file  string                          // 可选 JSON 持久化文件（-wordbank-file）
}

func NewWordBank(file string) *WordBank {
	wb := &WordBank{
		rooms: make(map[string]map[string]WordEntry),
		regex: make(map[string]*regexp.Regexp),
		file:  file,
	}
	if file != "" {
		wb.loadFile()
	}
	return wb
}

// Set 设置/覆盖一条房间词条目；isRegex 时即时编译（失败返回错误）。
func (wb *WordBank) Set(roomID string, e WordEntry) error {
	if e.Word == "" {
		return errEmptyWord
	}
	if e.Mode != "block" && e.Mode != "flag" {
		return errBadMode
	}
	var re *regexp.Regexp
	if e.IsRegex {
		compiled, err := regexp.Compile(e.Word)
		if err != nil {
			return err
		}
		re = compiled
	}
	wb.mu.Lock()
	if wb.rooms[roomID] == nil {
		wb.rooms[roomID] = make(map[string]WordEntry)
	}
	wb.rooms[roomID][e.Word] = e
	if re != nil {
		wb.regex[roomID+"|"+e.Word] = re
	} else {
		delete(wb.regex, roomID+"|"+e.Word)
	}
	wb.mu.Unlock()
	wb.saveFile()
	return nil
}

// Remove 删除一条房间词条目。
func (wb *WordBank) Remove(roomID, word string) {
	wb.mu.Lock()
	if r, ok := wb.rooms[roomID]; ok {
		delete(r, word)
		delete(wb.regex, roomID+"|"+word)
		if len(r) == 0 {
			delete(wb.rooms, roomID)
		}
	}
	wb.mu.Unlock()
	wb.saveFile()
}

// RoomWords 返回房间词条列表（副本，供 admin API 展示）。
func (wb *WordBank) RoomWords(roomID string) []WordEntry {
	wb.mu.RLock()
	defer wb.mu.RUnlock()
	r := wb.rooms[roomID]
	out := make([]WordEntry, 0, len(r))
	for _, e := range r {
		out = append(out, e)
	}
	return out
}

// Apply 对一条内容执行房间词库裁决：
//   - block 命中 → 返回打码后的内容（masked=true）
//   - flag 命中 → 内容原样返回，flagged=true
//
// 无房间配置时内容原样返回（全局 AC 过滤在调用方先行执行）。
func (wb *WordBank) Apply(roomID, content string) (masked string, flagged bool) {
	wb.mu.RLock()
	r := wb.rooms[roomID]
	if len(r) == 0 {
		wb.mu.RUnlock()
		return content, false
	}
	// 收集该房间的 block 与 flag 列表
	var blocks []WordEntry
	for _, e := range r {
		if e.Mode == "block" {
			blocks = append(blocks, e)
		}
		if e.Mode == "flag" && wordHitLocked(wb, roomID, e, content) {
			flagged = true
		}
	}
	wb.mu.RUnlock()

	out := content
	for _, e := range blocks {
		out = wb.mask(roomID, out, e)
	}
	return out, flagged
}

func wordHitLocked(wb *WordBank, roomID string, e WordEntry, content string) bool {
	if e.IsRegex {
		re := wb.regex[roomID+"|"+e.Word]
		return re != nil && re.MatchString(content)
	}
	return strings.Contains(content, e.Word)
}

// mask 对单条 block 规则打码。
func (wb *WordBank) mask(roomID, content string, e WordEntry) string {
	if e.IsRegex {
		wb.mu.RLock()
		re := wb.regex[roomID+"|"+e.Word]
		wb.mu.RUnlock()
		if re == nil {
			return content
		}
		return re.ReplaceAllString(content, "***")
	}
	return strings.ReplaceAll(content, e.Word, "***")
}

// --- 持久化（可选） ---

func (wb *WordBank) loadFile() {
	data, err := os.ReadFile(wb.file)
	if err != nil {
		return
	}
	var payload map[string][]WordEntry
	if err := json.Unmarshal(data, &payload); err != nil {
		log.Printf("[wordbank] load %s error: %v", wb.file, err)
		return
	}
	for roomID, entries := range payload {
		for _, e := range entries {
			if err := wb.Set(roomID, e); err != nil {
				log.Printf("[wordbank] load %s/%s error: %v", roomID, e.Word, err)
			}
		}
	}
	log.Printf("[wordbank] loaded %d rooms from %s", len(payload), wb.file)
}

func (wb *WordBank) saveFile() {
	if wb.file == "" {
		return
	}
	wb.mu.RLock()
	payload := make(map[string][]WordEntry, len(wb.rooms))
	for roomID, r := range wb.rooms {
		for _, e := range r {
			payload[roomID] = append(payload[roomID], e)
		}
	}
	wb.mu.RUnlock()
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return
	}
	if err := os.WriteFile(wb.file, data, 0644); err != nil {
		log.Printf("[wordbank] save error: %v", err)
	}
}

var (
	errEmptyWord = errWordBank("word 不能为空")
	errBadMode   = errWordBank("mode 只能是 block 或 flag")
)

type errWordBank string

func (e errWordBank) Error() string { return string(e) }
