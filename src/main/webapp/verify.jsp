<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<script type="text/javascript" src="<%=request.getContextPath() %>/js/jquery.js"></script>

<style type="text/css">
html, body {
    color: #333333;
    font-family: Verdana,Arial,Lucida,Helvetica,"宋体",sans-serif;
    font-size: 12px;
}
h1 {
	color: #666666;
}
.search {
	background: url("images/btn_search_out.png") no-repeat scroll 0 0 transparent;
    color: white;
    display: inline-block;
    height: 22px;
    line-height: 22px;
    margin-left: 5px;
    text-align: center;
    text-decoration: none;
    width: 74px;
}
.nav {
	border:1px solid #d4d4d4;
	border-right:0;
	display: inline-block;
	background:url(images/weekCon_bg.png) repeat-x 0 0;
}
.nav a {
    border-right: 1px solid #D4D4D4;
    color: #666666;
    display: inline-block;
    height: 22px;
    line-height: 22px;
    padding: 0 10px;
    text-decoration: none;
    vertical-align:top;
}
.nav a:hover{
	background:url(images/weekCon_bg.png) repeat-x 0 -23px;
}
.nav a.selected {
	background:url(images/weekCon_bg.png) repeat-x 0 -46px;
	color:#fff;
}
</style>
<title>Verify data in hbase</title>
</head>
<body>
<h1>HBASE TABLE NAVIGATOR</h1>
<h3>Step 1: select table </h3>
<div>
<span id="tables" class="nav"></span>
</div>
<h3>Step 2: select method </h3>
<div>
<span  id="methods"  class="nav"></span>
</div>
<h3>Step 3: input conditions </h3>
<div id="conditions">
<div id="cond_get" style="display: none">
row key: 
<input value="" />
<input value="" />
<input value="" />
<input value="" />
<input value="" />
</div>
<div id="cond_scan" style="display: none">
row number: 
<input id="rownum" value="10"/>
</div>
<div id="cond_count" style="display: none">
no condition for count, but count large table takes LONG time, please be patient!
</div>
</div>
<p />
<a href="javascript:void(0)" id="searchbtn" onclick="" class="search">Search</a>
<h2>Result</h2>
<div id="result">
</div>
<script>
var mapping;
var methods=["GET", "SCAN", "COUNT"];
var table;
var method;
var condition;
var url = "<%=request.getContextPath()%>/hbasenavi";
$(function() {

	//first lets get the table rowkey mapping
	$.getJSON(url, {}, function(data) {
			mapping = data;
			$.each( mapping, function(name, value){
				$("#tables").append("<a href='javascript:void(0)' id='"+name+"'>"+name+"</a>");
			});

			$('#tables a').bind('click',function() {
				 $(this).siblings().removeClass('selected');
				 $(this).addClass('selected');
				 table=$(this).attr("id");
			});
		});
	
	for (var i=0;i<methods.length;i++) {
		$("#methods").append("<a href='javascript:void(0)' id='"+methods[i]+"'>"+methods[i]+"</a>");
	}
	$('#methods a').bind('click',function(){
		 $(this).siblings().removeClass('selected');
		 $(this).addClass('selected');
		 var selectid = $(this).attr("id").toLowerCase();
		 method=selectid;
		 $("#cond_"+selectid).attr("style", "display:block");
		 $("#cond_"+selectid).siblings().attr("style", "display:none");
		 //append the inputs
	});
	
	$('#searchbtn').bind('click', function() {
		//get rownum and conditions;
		var rowNum, conditions;
		$("#result").empty();
		if (method=="scan") {
			rowNum=$('#rownum').attr("value");
			rowNum=parseInt(rowNum);
			if (rowNum<=0) {
				$("#result").append("<b>error:only positive number is allowed, man!</b>");
				return ;
			}
		} else if (method=="get") {
			conditions="";
			var noinput=true;
			$('#cond_get input').each(function(i) {
				var value=$.trim($(this).attr("value"));
				if (value!="") noinput=false;
				 conditions+=i+"."+value+"-";
			});
			if (noinput) {
				$("#result").append("<b>error:input some conditions, ok?</b>");
				return ;
			}
		} else if (method!="count") {
			$("#result").append("<b>error:please select a method, man!</b>");
			return;
		}
		$("#result").append("<b>search...., you may want to have a cup of coffee!</b>");
		$.getJSON(url,  {table: table, method: method, rowNum: rowNum, conditions:conditions},
				 function(data) {
			$("#result").empty();
			var res = data;
			var html ="<table>";
			if (res["fail"] != undefined) {
				$("#result").append(res["fail"]);
				return;
			}
			if (res instanceof Array) {
				 $.each(res, function(i,row){
					 $.each(row, function(rowkey, columns) {
						 html+="<tr><td>"+rowkey+"</td><td><ul>";
						$.each(columns, function(column, columnValue) {
							html+="<li>"+column+"===="+columnValue+"</li>";
						});
						 html+="</ul></td></tr>";
					 });
			 	});
			} else {
				html+="<tr><td>"+res+"</td></tr>";
			}
			html+="</table>";
			
			$("#result").append(html);
		});
	});
});
</script>
</body>
</html>