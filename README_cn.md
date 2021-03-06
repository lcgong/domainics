
概念上，领域(domain)是一种业务逻辑范围，一般包含领域对象和对外提供的服务。Domainics是一个采用了下列思想的Web服务应用的技术框架。我们的目的在于将领域体系化的应用到长效系统中，随需求演化，减少开发的技术债务。

Domainics是一个自造的单词，由**domain**(领域)和**-ics**(学问、体系)组合而成，表达下列思想。因此本框架是实现下列思想的技术框架。

## 信息系统domaincs行动纲要


所有业务逻辑均应封装在领域内

1. 领域内对象就是领域活动所依存的（信息）实体;

1. 领域对象有属性和集合，属性和集合也是领域对象，彼此会隐形的转化；
  张三的名字，名字是属性，也是对象，
  名字是单值的，也可能是多个的名字，即多值（Many-valued）或称为集合(Set).
  张三的名字是“张三”，张三的领域对象是人，名字“张三”既是人的属性也是名字的对象。
  张三的名字是张三、San Zhan，曾用张小三，这三个都是名字的对象，张三对象的属性有多个值，
  这三个名字相对张三这个人而言含义不同，现用名、英文名、曾用名。

1. DDD的聚合([aggregate](https://martinfowler.com/bliki/DDD_Aggregate.html))

1. 领域对象有名字，但名字不是唯一，名字是“能指”，对象实体是“所指”。

1. 对象的值，关心的是内容，但不应限制于唯一形式，
比如，出生日期，关注点是这个日子，而非表达形式，
例如：字符串'1980-1-1', 语言系统的日期对象 date(1980, 1 1) 或者表达为元年整数1234233。

1. 对象的值表达与媒体上，会强制或模糊的选定某可见的形式，凭借该媒体交流的各方均能解读。

1. 领域对象的可表达状态(representational state of domain object)，
数据所记录是状态，这是不可变的，不是领域对象，不是实体，也不是资源。
1. 对象的内容是连续的，由状态组成，不可表达和可表达。



* 在每个领域内应当寻找其简单的形式，保持稳定。领域之间的映射是复杂的，领域不应被这种映射所绑架。
* 每个领域都会有业务重心，由领域内一系列的服务体现。
* 领域是可以重叠的
* 领域对象承担保存状态容器的作用，会以某种实体的形态存在或代表，
但在服务中它的形态多变，因控制和处理要求而随之变化。
  > 如果困惑这点，尝试思考这个问题，在业务逻辑中应用面向对象设计为什么总感觉怪怪的，
  > 陷入无边的细节，需要不断抽象再抽象，抽象的结构的引发系统脆弱的风险也不小，时间旧了感觉还是ERD描述的信息更为持久。

* 领域服务需要资源，领域可以重叠，各领域的服务会彼此连接。
对某服务来说，它可以访问的都是资源，它不用操心资源管理问题。
* 领域内，资源的提供者是支撑起这个领域之“柱(pillar)”，pillar将承担领域内的资源管理。
* 领域服务接口是确定的，领域的边界由服务界定。
* 在长效系统中，服务会添加改变。
* 领域对象各形态和其实体的关系会变化。
* 领域边界和实体是自查而可文字显性的。
  *  领域是可以**自省(inspectable)**的，可以**自主**的得到本领域的服务接口和领域对象及其各型态的结构。
  *  无须额外的主数据管理，坚定程序即文档，文档可以从程序生成。
  *  必须考虑在长效系统中，维持文档可读且和程序一致的成本是高昂的。
* 多个版本的领域服务会在同期存在。
* 服务在产品周期内是独一无二的，不需要部署配置，要么卸载服务，要么安装服务，不存在部署配置。
  *  服务不奢望组件化，因为它依赖于领域，领域与系统的关系是复杂的。
  *  维护的成本总是要高于部署的成本，部署配置会提高维护成本，服务所依赖的基础是系统的资源以及环境偏好（存在多个偏好）。

可以参考[OData](http://www.odata.org) ，但OData走偏了，把问题搞复杂了。

#
