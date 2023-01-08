package main

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"net"
	"net/http"

	"sync"

	"net/rpc"
	"os"
	"time"
)

type Addre struct {
	Ip   string
	Port string
}
type RPC Addre
type FileI struct {
	FileName  string
	IsEnabled bool
	Key       [20]byte
}
type safeAddre struct {
	Addre *Addre
	ch    chan bool
}

var ClientMutex struct {
	sync.Mutex
}
var GetCMutex struct {
	sync.Mutex
}
var SMutex struct {
	sync.Mutex
}
var CMutex struct {
	sync.Mutex
}
var (
	Successor      *Addre
	Predecessor    safeAddre
	OldPrede       *Addre
	Fingers        [160]*Addre //chord論文中のk=1がk=0に対応
	checkingfinger int
	IsInitiallized bool
	Filelist       map[[20]byte]FileI
	Nilcnt         int
	ClientList     map[Addre]*rpc.Client

	Electioning, AmIboss, Isworking bool
	Self                            *Addre
	FixingList                      []int
)

const m = 160 //sha1のbit数
func main() {
	//defer profile.Start(profile.MemProfile).Stop()
	Filelist = make(map[[20]byte]FileI)
	Predecessor.ch = make(chan bool, 1)

	ClientList = make(map[Addre]*rpc.Client)
	Self = new(Addre)
	Successor = new(Addre)
	Predecessor.Addre = nil
	addrs, err := net.InterfaceAddrs() //OSのインターフェースを取ってくる
	if err != nil {
		panic(err)
	}
	e := flag.String("selfPort", "1234", "接続元Portの指定")
	f := flag.String("selfIP", "", "接続元IPの指定")
	g := flag.String("sucIP", "", "接続先ipの指定")
	h := flag.String("sucPort", "1234", "接続先portの指定") //この辺でオプションを読み取る

	flag.Parse()
	for _, a := range addrs { //自分のIP取得(Unix系向け)
		if Ipnet, ok := a.(*net.IPNet); ok && !Ipnet.IP.IsLoopback() { //たぶんOKは捨ててもいい
			if Ipnet.IP.To4() != nil {

				(*Self).Ip = Ipnet.IP.String()
				if *f != "" {
					(*Self).Ip = *f
				}

			}
		}
	}
	if *e != "" {
		(*Self).Port = *e
	} else {
		Log(fmt.Sprintln("e is empty. Select default port1234"))
		(*Self).Port = "1234"
	}

	//n.createに対応
	file, err := os.Create(Self.Ip + Self.Port + ".log") // ファイルを作成
	if err != nil {
		panic(err)
	}
	err = file.Close()
	if err != nil {
		panic(err)
	}
	Predecessor.Addre = nil
	OldPrede = nil
	*Successor = *Self

	ch := make(chan bool)  //Listener用の	channel
	go Listener(*Self, ch) //あらかじめ自分自身に接続できるようにしないとバグる
	time.Sleep(time.Second)

	if *g != "" { //接続先が指定されなければ自分一人でRingを形成
		time.Sleep(2 * time.Second)
		Successor.Ip = *g
		Successor.Port = *h

		serverAddress := (*Successor) //後で調整
		client, err := getClient(serverAddress)
		if *Successor == *Self {
			panic(errors.New("test"))
		}
		if err != nil {
			log.Fatal("dialing:", err)
		}

		err = Join(Successor, client)

		if err != nil {
			panic(err)
		}

	} else {
		Log(fmt.Sprintln("Ip or port are not input"))
	}

	fmt.Printf("(main)Predecessor: %v, Successor: %v\n", Predecessor.Addre, Successor)

	rand.Seed(time.Now().UnixNano())
	go func() { //StabilizeLoop

		for {
			time.Sleep(15*time.Second + time.Duration(rand.Intn(10000000000)))
			//time.Sleep(1*time.Second + time.Duration(rand.Intn(1000000000)))
			err = new(RPC).Stabilize(1, new(Addre))
			if err != nil {
				Log(fmt.Sprintln(err))
			}

		}
	}()
	//*/
	Fingers[0] = Successor
	Log("testz")
	go func() {
		for {
			time.Sleep(1000*time.Millisecond + 0)
			/*if Predecessor.Addre != nil && *(Predecessor.Addre) != *Self {//*/

			fix_fingers()

			//}

		}
	}()

	go func() {
		for {
			time.Sleep(15*time.Second + time.Duration(rand.Intn(30000000000)))
			checkPredecessor()
		}

	}()

	var arr [2]string
	var hoge Addre
	hoge.Ip = "127.0.0.1"
	hoge.Port = "1024"
	//client, err := getClient(hoge)
	shell := os.Getenv("Chordtest")
	if shell == "" {
		fmt.Println("環境変数 Chordtest が設定されていません")

	}
	if shell != "" {
		for {
			fmt.Scan(&arr[0], &arr[1]) // データを格納する変数のアドレスを指定
			if arr[1] == "" {
				fmt.Println("Invalid format")
				continue
			}
			now := time.Now()
			var answer Addre
			if arr[0] == "get" {
				buf := []byte(arr[1])
				q := sha1.Sum(buf)
				e := new(RPC).FindSuccessor(q, &answer)
				if e != nil {
					fmt.Println(e)
					continue
				}

				c, e := getClient(answer)
				if e != nil {
					fmt.Println(e)
					continue
				}
				a := new(FileI)
				e = c.Call("RPC.GetFile", q, a)
				if e != nil {
					fmt.Println(e)
					continue
				}
				fmt.Println(a)
				t := time.Since(now).Milliseconds()
				fmt.Printf("経過: %vms\n", t)
			} else if arr[0] == "put" {
				buf := []byte(arr[1])
				q := sha1.Sum(buf)
				e := new(RPC).FindSuccessor(q, &answer)
				if e != nil {
					fmt.Println(e)
					continue
				}

				c, e := getClient(answer)
				if e != nil {
					fmt.Println(e)
					continue
				}
				F := FileI{arr[1], true, q}
				e = c.Call("RPC.PutKey", F, new(bool))
				if e != nil {
					fmt.Println(e)
					continue
				}
				fmt.Println(F)
				t := time.Since(now).Milliseconds()
				fmt.Printf("put成功。経過: %vms\n", t)
			} else {
				fmt.Println("plz retry")
			}
			//ここから下は検索のベンチマーク 上をコメントアウトして適当にやってください
			/*
				for i := 0; i < cnt; i++ {
					ans := new(Addre)
					query := sha1.Sum([]byte(strconv.Itoa(i)))
					err = client.Call("RPC.FindSuccessor", query, ans)
					t := addr2SHA1((*ans))
					answer = answer + fmt.Sprintln(ans, new(big.Int).SetBytes(query[:]), new(big.Int).SetBytes(t[:]))
					if err != nil {
						fmt.Println(err)
					}
				}
				t := time.Since(now).Milliseconds()
				fmt.Println(answer)
				fmt.Printf("%v件検索、経過: %vms\n", cnt, t)
				//*/
		}
	}

	<-ch //listenerの終了を永久に待たせる
	Log(fmt.Sprintln(err))
	Log(fmt.Sprintln("worked on" + Self.Ip))
}

// これでRPCをListenする
func Listener(Self Addre, ch chan bool) error {
	addre := new(RPC)
	rpc.Register(addre)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp4", Self.Ip+":"+Self.Port)
	//l, e := net.Listen("tcp4", "127.0.0.1"+":"+Self.Port)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	err := http.Serve(l, nil)
	if err != nil {
		return err
	}
	fmt.Printf("error\n") //動かない前提
	ch <- true            //動かさない
	return nil
}

// n.Stabilizeに対応。
// やろうと思えば論文と同じ書き方で論文と同じような関数を実装できたが、
// 本質的ではない上に読みにくいのでGoのRPCでベタ書きすることにした。
func (a RPC) Stabilize(_ int, reply *Addre) error {
	Log(fmt.Sprintln("stabilize (Successor:", Successor, ")"))

	client, err := getClient(*Successor) //dial
	if err != nil {
		Log(fmt.Sprintln("0", err))
		return err
	}

	Successor_s_Pred := new(Addre)
	err = client.Call("RPC.ReplyPred", Self, Successor_s_Pred)
	if err != nil || Successor_s_Pred == nil {
		Log(fmt.Sprintln("1", err))
	}

	if cmp, ok := iskeyin(addr2SHA1(*Successor_s_Pred), addr2SHA1(*Self), addr2SHA1(*Successor)); !ok || Successor_s_Pred == nil {
		Log(fmt.Sprintln(ok, Successor_s_Pred))

	} else if (*Successor_s_Pred).Ip == "" {

		//Log(fmt.Sprintln("stabilize error", err)
	} else if cmp && *Successor_s_Pred != *Self /*Successor > Successor_s_Pred > Self*/ {
		*Successor = *Successor_s_Pred

	}

	r := new(Addre)
	err = client.Call("RPC.Notify", *Self, r)
	Log("SucNewPred:" + fmt.Sprintln(r))
	if err != nil {
		Log(fmt.Sprintln("2", err))

		return err
	}

	*reply = *Self

	Log(fmt.Sprintf("(stabilize) Successor:%v,Predecessor:%v", Successor, Predecessor.Addre))
	return nil
}

// n.fingersに対応。
// Fingerテーブルを更新する。ほとんど論文通りに実装できた。
// 論文と違う点は、クライアントを閉じる処理をこの関数に追加した。
// クライアントをきちんと閉じないと、メモリリーク･goroutineリークが起こって簡単にOOMキラーが発動する。
func fix_fingers() {

	checkingfinger = checkingfinger + 1
	if checkingfinger >= m {
		checkingfinger = 0
	}

	bigNum := big.NewInt(int64(checkingfinger))
	bigTwo := big.NewInt(2)
	bigM := big.NewInt(m)

	z := bigNum.Exp(bigTwo, bigNum, new(big.Int).Exp(bigTwo, bigM, nil))
	if Fingers[checkingfinger] == nil {
		Fingers[checkingfinger] = new(Addre)
	}
	q := (idadd(addr2SHA1(*Self), z))
	err := new(RPC).FindSuccessor(q, Fingers[checkingfinger])

	if err != nil {
		Log(fmt.Sprintln(err))
		return
	}
	if Fingers[checkingfinger].Ip == "" {
		Fingers[checkingfinger] = nil
	}

	if checkingfinger == 0 {
		file, err := os.Create(Self.Ip + Self.Port + ".txt") // ファイルを作成
		if err != nil {
			Log(fmt.Sprintln("4", err))
			panic(err)
		}

		err = file.Close()
		if err != nil {
			Log(fmt.Sprintln("5", err))
		}

		f, err := os.OpenFile(Self.Ip+Self.Port+".txt", os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			Log(fmt.Sprintln("6", err))
			return
		}
		newLine := fmt.Sprintf("%v [shape = box]; ", Self.Port)
		_, err = fmt.Fprintln(f, newLine)
		if err != nil {
			Log(fmt.Sprintln("8", err))
		}
		newLine = fmt.Sprintf(" %v -> %v [style = \"solid\", label = \"next\"];", Self.Port, Fingers[1].Port)
		_, err = fmt.Fprintln(f, newLine)
		if err != nil {
			Log(fmt.Sprintln("9", err))
		}

		_, err = fmt.Fprintf(f, "	%v", q)
		if err != nil {
			Log(fmt.Sprintln("9", err))
		}
		for i := 158; i < m; i++ {
			if Fingers[i] != nil {

				newLine := fmt.Sprintf("%v -> %v [style = \"dashed\", label = \"%v\"] ;", Self.Port, Fingers[i].Port, i)
				_, err = fmt.Fprintln(f, newLine)
				if err != nil {
					Log(fmt.Sprintln("9", err))
				}
			}
		}
		//クライアント閉じる

		if checkingfinger == 100 {
			SMutex.Lock()
			defer SMutex.Unlock()
			for k, c := range ClientList {
				if (Predecessor.Addre != nil && k == *Predecessor.Addre) || k == *Self || k == *Successor {
					continue
				}
				inlist := false
				for i := 0; i < 160; i++ {
					if Fingers[i] != nil && k != *Fingers[i] {
						continue
					} else {
						inlist = true
					}
				}
				if inlist {
					continue
				} else {
					e := c.Close()
					if e != nil {
						Log(fmt.Sprintln(e))
					}
				}
			}
		}

	}
}

// このノードが持っている情報をもとに最も近いノードを探す。
func closestPrecedingNode(query [20]byte) (ad *Addre, ok bool) {
	for i := m - 1; i >= 0; i-- {
		if Fingers[i] != nil {
			fingerbyte := addr2SHA1(*Fingers[i])
			selfbyte := addr2SHA1(*Self)
			if in, ok := iskeyin((fingerbyte), selfbyte, query); in && ok {
				return Fingers[i], true
			}
		}

	}
	return Self, true
}

// クライアントを取得する。
// 逐一取得しても一応動作するが、無駄にgoroutineが増えるので非推奨。
// マップで一括管理するのでマップを同時に書き込みすることのないようロックをかけた。
// GoのMutexは不安定なイメージがあるが、channelよりこちらのほうがGoroutineを増やさず済む。
func getClient(query Addre) (client *rpc.Client, err error) {
	GetCMutex.Lock()
	defer GetCMutex.Unlock()

	client, doesexist := ClientList[query]
	if doesexist && client != nil {
		err := client.Call("RPC.Replya", Self, new(string))
		if err == nil {
			return client, nil
		} else {
			err = client.Close()
			if err != nil {
				Log(fmt.Sprintln("getC", err))
			}
		}

	}
	//Log(fmt.Sprintln("getClient:", query))
	client, err = rpc.DialHTTP("tcp4", query.Ip+":"+query.Port)
	ClientList[query] = client

	if err != nil {
		Log(fmt.Sprintln("Error connecting"))
		return nil, err
	}
	return client, nil
}

// 自身のPredecessorがきちんと動作しているか確認。
// なくても一応動く。
func checkPredecessor() {
	if Predecessor.Addre != nil {
		CMutex.Lock()
		defer CMutex.Unlock()
		client, err := getClient(*Predecessor.Addre)

		if err != nil {
			Predecessor.Addre = nil
			//ClientMutex.Unlock()
			return
		} else {
			err = client.Call("RPC.Replya", Self, new(string))
			if err != nil {
				Predecessor.Addre = nil
				//ClientMutex.Unlock()
				return
			} else {
				//ClientMutex.Unlock()
				defer client.Close()
			}
		}
		//ClientMutex.Unlock()
	}
}

// アドレスをSHA-1ハッシュするだけ
func addr2SHA1(addr Addre) [20]byte {
	addrByte := []byte(addr.Ip + ":" + addr.Port)
	tmp := sha1.Sum(addrByte)
	return tmp
}

// アドレス2つを比較する。addr1>addr2ならばcmp==1,=ならばcmp==0、<ならばcmp==-1。
func AddreComp(addr1 *Addre, addr2 *Addre) (cmp int, ok bool) {
	//要調整
	if addr1 == nil || addr2 == nil {
		return 0, false
	}
	if addr1.Ip == "" || addr2.Ip == "" || addr1.Port == "" || addr2.Port == "" {
		return 0, false
	}

	hashing1 := addr2SHA1(*addr1)
	comp1 := new(big.Int)
	Num1, ok := comp1.SetString((hex.EncodeToString(hashing1[:])), 16)
	if !ok {
		Log(fmt.Sprintln("(AddreComp)SetString: error"))
		return 0, false
	}

	hashing2 := addr2SHA1(*addr2)
	comp2 := new(big.Int)
	Num2, ok := comp2.SetString((hex.EncodeToString(hashing2[:])), 16)
	if !ok {
		Log(fmt.Sprintln("(AddreComp)SetString: error"))
		return 0, false
	}

	return Num1.Cmp(Num2), true
}

// a>b->1、a=b->0、a<b->-1
func bytecmp(a, b []byte) (int, bool) {
	//a>b ->1
	Num1 := new(big.Int).SetBytes(a[:])
	Num2 := new(big.Int).SetBytes(b[:])
	return Num1.Cmp(Num2), true
}

// query∈[smaller,larger]を返す。
// 実際にsmaller<largerである必要はなく、論文同様に扱える。
func iskeyin(query [20]byte, smaller [20]byte, larger [20]byte) (cmp bool, ok bool) {
	if smaller == larger || smaller == query || larger == query { //厳密には異なる
		return true, true
	}
	if cmp, ok := bytecmp(smaller[:], larger[:]); ok && cmp == 1 {
		//smaller>largerのときの処理
		cmp1, ok1 := bytecmp(query[:], smaller[:])
		cmp2, ok2 := bytecmp(query[:], larger[:])
		if ok1 && ok2 && ((cmp1 == 1) || (cmp2 == -1)) {
			return true, true
		} else if ok1 && ok2 && ((cmp1 == -1) && (cmp2 == 1)) {
			return false, true
		}

	} else if ok && cmp == -1 {
		//smaller<largerのときの処理
		cmp1, ok1 := bytecmp(query[:], smaller[:])
		cmp2, ok2 := bytecmp(query[:], larger[:])
		if ok1 && ok2 && ((cmp1 == 1) && (cmp2 == -1)) {
			return true, true
		} else if ok1 && ok2 && ((cmp1 == -1) || (cmp2 == 1)) {
			return false, true
		}
	}
	return false, false
}

// もう少しやりようはあったように思う。具体的にはすべてのハッシュ値を整数で扱うなど。今後の課題。
// sha-1ハッシュと整数を加算したいときに使う。
func idadd(x [20]byte, y *big.Int) [20]byte {
	var tmp [20]byte
	z := new(big.Int)
	z = z.SetBytes(x[:])
	z = z.Add(y, z)
	for i := 0; i < len(z.Bytes()) && i < 20; i++ {
		tmp[i] = z.Bytes()[i]
	}
	return tmp
}

// n.joinに対応。clientはcontact nodeのクライアント。つながればどこでもいい。
// ただしcloseはfixfinger時にしか行わないので注意。
func Join(np *Addre, client *rpc.Client) error {
	n := Self
	Predecessor.Addre = nil
	Predecessor.ch <- true
	defer func() { <-Predecessor.ch }()
	defer Log("joined" + fmt.Sprintln(Self))
	Predecessor.Addre = nil
	hoge := addr2SHA1(*n)
	time.Sleep(1000 * time.Millisecond)

	err := client.Call("RPC.FindSuccessor", hoge, Successor)
	Log(fmt.Sprintln(Successor))
	if err != nil {
		panic(err)
	}
	if *Successor == *Self {
		panic("test1")
	}
	client1, err := getClient(*Successor)
	if err != nil {
		Log(fmt.Sprintln("10", err))

	}
	err = client1.Call("RPC.LockS", Self, new(bool))
	if err != nil {
		Log(fmt.Sprintln("11", err))
	} else {
		defer client1.Call("RPC.ULockS", true, new(bool))
	}

	err = client1.Call("RPC.Notify", Self, new(Addre))
	if err != nil {
		Log(fmt.Sprintln("12", err))

		return nil
	}

	Log("test")

	return nil
}
func Log(logstr string) error {
	f, err := os.OpenFile(Self.Ip+Self.Port+".log", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(f, logstr)
	if err != nil {
		return err
	}
	return nil
}

//~~~~~以下RPC用関数~~~~~

// 自分のPredecessorを"返す"だけ。
// GolangのRPCは非常に使い勝手が悪く、返り値にerror型しか返せないようなのでこんなきったない実装になる。
func (a RPC) ReplyPred(n *Addre, reply *Addre) error {
	if Predecessor.Addre == nil {

		return errors.New("Reply_pred:Predecessor is nil")
	}
	*reply = *(Predecessor.Addre)

	return nil
}

// Notifyしてきた者が自分のPredecessorとして適格かどうかを判定し、そうでないならエラーを返す。
// 適格ならPredecessorに代入。
func (a RPC) Notify(predCandidate Addre, reply *Addre) error {
	Log(fmt.Sprintln("notified by ", predCandidate, "My Pred:", Predecessor.Addre))
	if predCandidate == *Self {
		*reply = Addre{}
		return nil
	}
	if Predecessor.Addre == nil {

		Predecessor.Addre = &predCandidate
		*reply = Addre{}
		return nil
	} else if *Predecessor.Addre == *Self {
		Predecessor.Addre = nil
		*reply = *(Predecessor.Addre)
		return nil
	}
	if cmp, ok := iskeyin(addr2SHA1(predCandidate), addr2SHA1(*(Predecessor.Addre)), addr2SHA1(*Self)); ok && cmp && !(predCandidate == *Self) {
		OldPrede = new(Addre)
		*OldPrede = *(Predecessor.Addre)
		Predecessor.Addre = &predCandidate
		*reply = *(Predecessor.Addre)
		return nil
	} else {
		Log(fmt.Sprintln(predCandidate))
	}

	return errors.New("you are not my predecessor")
}

// 主に死活管理用。pingの代わり。
func (a RPC) Replya(n *Addre, reply *string) error {
	tekitou := "a"
	*reply = tekitou
	return nil
}

// queryの次のノードを探してくる関数。論文参照。
func (a RPC) FindSuccessor(query [20]byte, address *Addre) error {

	if cmp, ok := iskeyin(query, addr2SHA1(*Self), addr2SHA1(*Successor)); (cmp && ok) && (query != addr2SHA1(*Self)) {
		*address = *Successor
		return nil
	} else if !ok {
		Log(fmt.Sprintln("(Find_Suc)iskeyin: error"))
		return errors.New("(Find_Suc)iskeyin: error")
	} else if !cmp {
		ans, ok := closestPrecedingNode(query)

		if ok && ans != nil {
			if *ans != *Self {
				client, err := getClient(*ans)

				if err != nil {
					return err
				}
				ans1 := new(Addre)
				err = client.Call("RPC.FindSuccessor", query, ans1)

				if err != nil {
					Log(fmt.Sprintln(err))
				}

				*address = *ans1

			}

		} else {
			return errors.New("(Find_Suc)closestprecedingnode: error")
		}

	}
	return nil
}

// Join時にPredecessorがおかしくならないよう、ロックをかける。
// 常識的なJoinNode/secondを仮定するなら別に要らないが、一応実装した。
func (a RPC) LockS(b *Addre, re *bool) error {
	Predecessor.ch <- true
	return nil
}

// アンロック。
func (a RPC) ULockS(t bool, re *bool) error {
	<-Predecessor.ch
	return nil
}

// queryとなるファイルの保存を依頼する関数。実際のファイルトランザクションは未実装。
func (a RPC) PutKey(query FileI, reply *bool) error {
	couldBeStored, ok := iskeyin(query.Key, addr2SHA1(*Predecessor.Addre), addr2SHA1(*Self))
	if !ok {
		return errors.New("error")
	} else if !couldBeStored {
		return errors.New("it's not my occupation")
	} else {
		Filelist[query.Key] = query
		*reply = true
		return nil
	}
}

// そのノードがファイルを保持しているのならファイルを、そうでないのならエラーを"返す"。
// 実際のファイルトランザクションは未実装。
func (a RPC) GetFile(query [20]byte, reply *FileI) error {
	obj, ok := Filelist[query]
	if !ok {
		return errors.New("not found")
	} else {
		*reply = obj
		return nil
	}
}
