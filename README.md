## SHP TO MySQL
通过Java代码，将shp文件导入到mysql数据库。因为没有找到现成的工具，因此就根据网上资料，抄了一段可以跑起来的代码使用

### 1.目录结构
其中目录结构如下

- lib, 相关的依赖
- shps, 上海市的shp文件
- src, 主要代码地方，其实就是APP.java里面的代码
- pom.xml, 依赖列举出来

### 2.使用

直接在APP.java中找到修改的存放shp的路径，然后运行即可

### 3.注意

- GBK读取shp文件
- MySQL服务，建议设置好参数：character-set-server=utf8
