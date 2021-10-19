#time "on"
#r "nuget: Akka.Remote, 1.4.25"
#r "nuget: Akka, 1.4.25"
#r "nuget: Akka.FSharp, 1.4.25"
#r "nuget: Akka.Serialization.Hyperion, 1.4.25"
open System.Security.Cryptography
open System
open Akka.Actor
open Akka.FSharp 
open Akka.Configuration

let workers = 8

type ActorMsg =
    | WorkerMsg of string*int*int*int*int
    | WorkerMsgClient of string*int*int*int*int*int
    | DispatcherMsg of string*int
    | EndMsg

// Configuration
let configuration = 
    ConfigurationFactory.ParseString(
        @"akka {
            log-config-on-start : on
            stdout-loglevel : DEBUG
            loglevel : ERROR

            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
                debug : {
                    receive : on
                    autoreceive : on
                    lifecycle : on
                    event-stream : on
                    unhandled : on
                }
            }
            remote {
                helios.tcp {
                    port = 8778
                    hostname = 10.20.142.27
                }
            }
        }")
let mutable actN = ""

printfn "%s" actN
let rec appendAndIterate(bs: string, l: int, start: int, stop: int, N: int, myseq: array<string>) =
    if myseq.[0] = "" && l > 0 then 
        for i in start .. stop do
            if myseq.[0] = "" then 
                let lessL = l - 1
                let ebs = bs + string (char i)
                appendAndIterate (ebs, lessL, start, stop, N, myseq)
                if l = 1 then
                    let s = System.Text.Encoding.ASCII.GetBytes(ebs) |> (new SHA256Managed()).ComputeHash |> Array.map (fun (x : byte) -> System.String.Format("{0:x2}", x)) |> String.concat String.Empty
                    let firstN = s.[0 .. N-1]
                    // match firstN with
                    // | actN -> ebs 
                    if firstN.Equals(actN) then
                        // printfn "%s %s" ebs s
                        myseq.[0] <- (ebs + " " + s)
                    else
                        myseq.[0] <- ""

let system = ActorSystem.Create("RemoteMiner", configuration)
type RemoteChildActor() =
    inherit Actor()
    override x.OnReceive message =
        let x : ActorMsg = downcast message
        let sref = system.ActorSelection ("akka.tcp://Miner@10.20.142.27:8777/user/boss")
        let pref = system.ActorSelection("akka.tcp://Miner@10.20.142.27:8777/user/PrinterActor")
        match x with
        | WorkerMsg(bs,count, start, stop, N) -> let ans = [|""|]
                                                 appendAndIterate (bs, count, start, stop, N, ans)
                                                 printfn "%s" ans.[0]
                                                 //pref <! "Remote Miner: " + ans.[0]
                                                 pref <! "Done"
        | _ -> printfn "Worker Received Wrong message"

let RemoteBossActor = 
    spawn system "RemoteBossActor"
    <| fun mailbox ->
        let actcount = Environment.ProcessorCount |> int64
        let tot = actcount*1L
        let remoteChildActorsPool = 
                [1L .. tot]
                |> List.map(fun id -> system.ActorOf(Props(typedefof<RemoteChildActor>)))

        let childenum = [|for rp in remoteChildActorsPool -> rp|]
        let workerSystem = system.ActorOf(Props.Empty.WithRouter(Akka.Routing.RoundRobinGroup(childenum)))
        
        let rec loop() =
            actor {
                let! (message:obj) = mailbox.Receive()
                let (bs, count, start, mid, stop, N) : Tuple<string, int,int,int,int,int> = downcast message
                actN <- ""
                for j in 1 .. N do
                    actN <- actN + "0"
                for i in mid .. stop do
                    //for j in start .. stop do
                    let ebs = bs + string (char i) //+ string (char j)
                    workerSystem <! WorkerMsg(ebs, count, start, stop, N)
                return! loop()    
            }
        printfn "Remote Boss Started \n" 
        loop()
// system.WhenTerminated.Wait()
System.Console.ReadLine() |> ignore