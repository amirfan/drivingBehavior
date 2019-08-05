package com.sssjd.configure

object IpEnum extends Enumeration {
  type IpEnum = Value
  val DevelopEnv = Value("192.168.")
  val awsEnv = Value("172.31.")
}
