
## CRUD

* GET 得到对象的状态，得到对象的那些状态，边界是模糊的，不应追求完备，而是够用的
* PUT 更新对象的状态
  所更新的内容没有完整性这个概念，所有的更新(PUT)都是部分的，因此没必要采用PATCH。
* POST 对象从无到有，一个有状态的对象
* DELETE 指定的对象被消灭掉，从有到无

### GET 获取方法

```
GET /invoices

HTTP/1.1 200 OK
[
  {
  },
]  
```

```
GET /invoices/684

HTTP/1.1 200 OK
{
}
```

### PUT 更新方法

```
PUT /invoices/684
{
  "address": "address is modified",
}

HTTP/1.1 200 Ok
```
因此，客户端发送PUT后，其状态并没有变化，因此没必要返回同样的消息，
客户端也没必要进行更新。

### DELETE 删除方法

DELETE方法需要保留等幂性，多次删除同一个实体，效果是一样的。
```
DELETE /invoices/684

HTTP/1.1 202 Accepted
```

### POST 创建方法

```sh
POST /invoices/new
{

}

HTTP/1.1 201 Created
Location: /invoices/684
{
  id : 684,
  ...
}
```
POST 更新状态，并导致新的状态产生，作为反作用需要将该更新的状态返回。

例如，更新工作流的状态，因为approve事件的发生会导致实体状态发生变化
```
POST /invoices/684/approve
{
  "status": "approved",
}

HTTP/1.1 201 Created
Location: /invoices/684
{
  "status": "pending"
}
```
客户端发送POST后，也需要根据返回的内容，更新自己。


### 404 Not Found

GET / PUT / POST 若所操作的对象不存在，则返回404错误。DELETE是没必要返回该错误的。
