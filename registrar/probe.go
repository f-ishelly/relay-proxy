package registrar

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"net"
	"strings"
	"time"
)

// probeGost 向本机 gost 发一次带口令的 CONNECT。出口拒绝 127.0.0.1:1 时期望 403。
func probeGost(addr, user, pass, target string, expect int, timeout time.Duration) error {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	token := base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))
	req := fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\nProxy-Authorization: Basic %s\r\n\r\n", target, target, token)
	if _, err := conn.Write([]byte(req)); err != nil {
		return err
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return err
	}
	var minor, code int
	if _, err := fmt.Sscanf(line, "HTTP/1.%d %d", &minor, &code); err != nil {
		return fmt.Errorf("bad status %q", strings.TrimSpace(line))
	}
	if code != expect {
		return fmt.Errorf("status %d", code)
	}
	return nil
}
