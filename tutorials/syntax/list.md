---
title: code syntax code code code test code
description: 1code code
tags: [products>sap-hana-cloud-platform, topic>cloud, topic>java]
primary_tag: tutorial:product/sapHana
---
 
  **Example:without code** 
```
let apples = 3
let oranges = 5
let appleSummary = "I h1ave \(apples) apples."
let fruitSummary = "I have \(apples + oranges) pieces of fruit."
class TriangleAndSquare {
    var triangle: EquilateralTriangle {
        willSet {
            square.sideLength = newValue.sideLength
        }
    }
    var square: Square {
        willSet {
            triangle.sideLength = newValue.sideLength
        }
    }
    init(size: Double, name: String) {
        square = Square(sideLength: size, name: name)
        triangle = EquilateralTriangle(sideLength: size, name: name)
    }
}
var triangleAndSquare = TriangleAndSquare(size: 10, name: "another test shape")
triangleAndSquare.square.sideLength
triangleAndSquare.triangle.sideLength
triangleAndSquare.square = Square(sideLength: 50, name: "larger square")
triangleAndSquare.triangle.sideLength
enum ServerResponse {
    case Result(String, String)

 }
7.simpleDescription
```

 
 **Example:Swift** 
```swift
let apples = 3
let oranges = 5
let appleSummary = "I have \(apples) apples."
let fruitSummary = "I have \(apples + oranges) pieces of fruit."
class TriangleAndSquare {
    var triangle: EquilateralTriangle {
        willSet {
            square.sideLength = newValue.sideLength
        }
    }
    var square: Square {
        willSet {
            triangle.sideLength = newValue.sideLength
        }
    }
    init(size: Double, name: String) {
        square = Square(sideLength: size, name: name)
        triangle = EquilateralTriangle(sideLength: size, name: name)
    }
}
var triangleAndSquare = TriangleAndSquare(size: 10, name: "another test shape")
triangleAndSquare.square.sideLength
triangleAndSquare.triangle.sideLength
triangleAndSquare.square = Square(sideLength: 50, name: "larger square")
triangleAndSquare.triangle.sideLength
enum ServerResponse {
    case Result(String, String)
    case Error(String)
}

let success = ServerResponse.Result("6:00 am", "8:09 pm")
let failure = ServerResponse.Error("Out of cheese.")

switch success {
    case let .Result(sunrise, sunset):
        let serverResponse = "Sunrise is at \(sunrise) and sunset is at \(sunset)."
    case let .Error(error):
        let serverResponse = "Failure...  \(error)"
}
extension Int: ExampleProtocol {
    var simpleDescription: String {
        return "The number \(self)"
    }
    mutating func adjust() {
        self += 42
    }
 }
7.simpleDescription
```

 **Example:javascript** 

```javascript
rg) {
  20 line
  quit;
function fancyAlert(arg) { function fancyAlert(arg) {function fancyAlert(arg) {function fancyAlert(arg) {function fancyAlert(arg) {function fancyAlert(arg) {function fancyAlert(arg) { 
  if(arg) {
    $.facebox({div:'#foo'})
  }
}
function fancyAlert(arg) {
  if(arg) {
    $.facebox({div:'#foo'})
  }
}
function fancyAlert(arg) {
  if(arg) {
    $.facebox({div:'#foo'})
  }
}
function fancyAlert(arg) {
  if(arg) {
  40 line
  quit;
function fancyAlert(arg) { function fancyAlert(arg) {function fancyAlert(arg) {function fancyAlert(arg) {function fancyAlert(arg) {function fancyAlert(arg) {function fancyAlert(arg) { 
  if(arg) {
    $.facebox({div:'#foo'})
  }
}
function fancyAlert(arg) {
  if(arg) {
    $.facebox({div:'#foo'})
  }
}
function fancyAlert(arg) {
  if(arg) {
    $.facebox({div:'#foo'})
  }
}
function fancyAlert(arg) {
  if(arg) {
  60 line
  quit;
function fancyAlert(arg) { function fancyAlert(arg) {function fancyAlert(arg) {function fancyAlert(arg) {function fancyAlert(arg) {function fancyAlert(arg) {function fancyAlert(arg) { 
  if(arg) {
    $.facebox({div:'#foo'})
  }
}
function fancyAlert(arg) {
  if(arg) {
    $.facebox({div:'#foo'})
  }
}
function fancyAlert(arg) {
  if(arg) {
    $.facebox({div:'#foo'})
  }
}
function fancyAlert(arg) {
  if(arg) {
  60 line
  quit;
function fancyAlert(arg) { function fancyAlert(arg) {function fancyAlert(arg) {function fancyAlert(arg) {function fancyAlert(arg) {function fancyAlert(arg) {function fancyAlert(arg) { 
  if(arg) {
    $.facebox({div:'#foo'})
  }
}
function fancyAlert(arg) {
  if(arg) {
    $.facebox({div:'#foo'})
  }
}
function fancyAlert(arg) {
  if(arg) {
    $.facebox({div:'#foo'})
  }
}
function fancyAlert(arg) {
  if(arg) {
  80 line
  quit;
function fancyAlert(arg) { function fancyAlert(arg) {function fancyAlert(arg) {function fancyAlert(arg) {function fancyAlert(arg) {function fancyAlert(arg) {function fancyAlert(arg) { 
  if(arg) {
    $.facebox({div:'#foo'})
  }
}
function fancyAlert(arg) {
  if(arg) {
    $.facebox({div:'#foo'})
  }
}
function fancyAlert(arg) {
  if(arg) {
    $.facebox({div:'#foo'})
  }
}
function fancyAlert(arg) {
  if(arg) {
  100 line
  quit;
function fancyAlert(arg) { function fancyAlert(arg) {function fancyAlert(arg) {function fancyAlert(arg) {function fancyAlert(arg) {function fancyAlert(arg) {function fancyAlert(arg) { 
  if(arg) {
    $.facebox({div:'#foo'})
  }
}
function fancyAlert(arg) {
  if(arg) {
    $.facebox({div:'#foo'})
  }
}
```

**Example:xml** 

```xml
<?xml version="1.0" encoding="utf-8"?>
<Project ToolsVersion="12.0" DefaultTargets="Build" xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
  <Import Project="$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props" Condition="Exists('$(MSBuildExtensionsPath)\$(MSBuildToolsVersion)\Microsoft.Common.props')" />
  <PropertyGroup>
    <Configuration Condition=" '$(Configuration)' == '' ">Debug</Configuration>
    <Platform Condition=" '$(Platform)' == '' ">AnyCPU</Platform>
    <ProjectGuid>{99D9BF15-2911-4D10-8079-83ABAD688E8B}</ProjectGuid>
    <OutputType>Exe</OutputType>
    <AppDesignerFolder>Properties</AppDesignerFolder>
    <RootNamespace>csproj_sample</RootNamespace>
    <AssemblyName>csproj-sample</AssemblyName>
    <TargetFrameworkVersion>v4.5.1</TargetFrameworkVersion>
    <FileAlignment>512</FileAlignment>
    <AutoGenerateBindingRedirects>true</AutoGenerateBindingRedirects>
  </PropertyGroup>
  <PropertyGroup Condition=" '$(Configuration)|$(Platform)' == 'Debug|AnyCPU' ">
    <PlatformTarget>AnyCPU</PlatformTarget>
    <DebugSymbols>true</DebugSymbols>
    <DebugType>full</DebugType>
    <Optimize>false</Optimize>
    <OutputPath>bin\Debug\</OutputPath>
    <DefineConstants>DEBUG;TRACE</DefineConstants>
    <ErrorReport>prompt</ErrorReport>
    <WarningLevel>4</WarningLevel>
  </PropertyGroup>
  <PropertyGroup Condition=" '$(Configuration)|$(Platform)' == 'Release|AnyCPU' ">
    <PlatformTarget>AnyCPU</PlatformTarget>
    <DebugType>pdbonly</DebugType>
    <Optimize>true</Optimize>
    <OutputPath>bin\Release\</OutputPath>
    <DefineConstants>TRACE</DefineConstants>
    <ErrorReport>prompt</ErrorReport>
    <WarningLevel>4</WarningLevel>
  </PropertyGroup>
  <ItemGroup>
    <Reference Include="System" />
    <Reference Include="System.Core" />
    <Reference Include="System.Xml.Linq" />
    <Reference Include="System.Data.DataSetExtensions" />
    <Reference Include="Microsoft.CSharp" />
    <Reference Include="System.Data" />
    <Reference Include="System.Xml" />
  </ItemGroup>
  <ItemGroup>
    <Compile Include="Program.cs" />
    <Compile Include="Properties\AssemblyInfo.cs" />
  </ItemGroup>
  <ItemGroup>
    <None Include="App.config" />
  </ItemGroup>
  <Import Project="$(MSBuildToolsPath)\Microsoft.CSharp.targets" />
  <!-- To modify your build process, add your task inside one of the targets below and uncomment it. 
       Other similar extension points exist, see Microsoft.Common.targets.
  <Target Name="BeforeBuild">
  </Target>
  <Target Name="AfterBuild">
  </Target>
  -->
</Project>
```
**Example:java 625 rows** 
```java
// This is a generated file. Not intended for manual editing.
package org.intellij.grammar.parser;

import com.intellij.lang.PsiBuilder;
import com.intellij.lang.PsiBuilder.Marker;
import static org.intellij.grammar.psi.BnfTypes.*;
import static org.intellij.grammar.parser.GeneratedParserUtilBase.*;
import com.intellij.psi.tree.IElementType;
import com.intellij.lang.ASTNode;
import com.intellij.psi.tree.TokenSet;
import com.intellij.lang.PsiParser;
import com.intellij.lang.LightPsiParser;

@SuppressWarnings({"SimplifiableIfStatement", "UnusedAssignment"})
public class GrammarParser implements PsiParser, LightPsiParser {

  public ASTNode parse(IElementType t, PsiBuilder b) {
    parseLight(t, b);
    return b.getTreeBuilt();
  }

  public void parseLight(IElementType t, PsiBuilder b) {
    boolean r;
    b = adapt_builder_(t, b, this, EXTENDS_SETS_);
    Marker m = enter_section_(b, 0, _COLLAPSE_, null);
    if (t == BNF_ATTR) {
      r = attr(b, 0);
    }
    else if (t == BNF_ATTR_PATTERN) {
      r = attr_pattern(b, 0);
    }
    else if (t == BNF_ATTR_VALUE) {
      r = attr_value(b, 0);
    }
    else if (t == BNF_ATTRS) {
      r = attrs(b, 0);
    }
    else if (t == BNF_CHOICE) {
      r = choice(b, 0);
    }
    else if (t == BNF_EXPRESSION) {
      r = expression(b, 0);
    }
    else if (t == BNF_LITERAL_EXPRESSION) {
      r = literal_expression(b, 0);
    }
    else if (t == BNF_MODIFIER) {
      r = modifier(b, 0);
    }
    else if (t == BNF_PAREN_EXPRESSION) {
      r = paren_expression(b, 0);
    }
    else if (t == BNF_PREDICATE) {
      r = predicate(b, 0);
    }
    else if (t == BNF_PREDICATE_SIGN) {
      r = predicate_sign(b, 0);
    }
    else if (t == BNF_QUANTIFIED) {
      r = quantified(b, 0);
    }
    else if (t == BNF_QUANTIFIER) {
      r = quantifier(b, 0);
    }
    else if (t == BNF_REFERENCE_OR_TOKEN) {
      r = reference_or_token(b, 0);
    }
    else if (t == BNF_RULE) {
      r = rule(b, 0);
    }
    else if (t == BNF_SEQUENCE) {
      r = sequence(b, 0);
    }
    else if (t == BNF_STRING_LITERAL_EXPRESSION) {
      r = string_literal_expression(b, 0);
    }
    else {
      r = parse_root_(t, b, 0);
    }
    exit_section_(b, 0, m, t, r, true, TRUE_CONDITION);
  }

  protected boolean parse_root_(IElementType t, PsiBuilder b, int l) {
    return grammar(b, l + 1);
  }

  public static final TokenSet[] EXTENDS_SETS_ = new TokenSet[] {
    create_token_set_(BNF_LITERAL_EXPRESSION, BNF_STRING_LITERAL_EXPRESSION),
    create_token_set_(BNF_CHOICE, BNF_EXPRESSION, BNF_LITERAL_EXPRESSION, BNF_PAREN_EXPRESSION,
      BNF_PREDICATE, BNF_QUANTIFIED, BNF_REFERENCE_OR_TOKEN, BNF_SEQUENCE,
      BNF_STRING_LITERAL_EXPRESSION),
  };

  /* ********************************************************** */
  // id attr_pattern? '=' attr_value ';'?
  public static boolean attr(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "attr")) return false;
    boolean r, p;
    Marker m = enter_section_(b, l, _NONE_, "<attr>");
    r = consumeToken(b, BNF_ID);
    p = r; // pin = 1
    r = r && report_error_(b, attr_1(b, l + 1));
    r = p && report_error_(b, consumeToken(b, BNF_OP_EQ)) && r;
    r = p && report_error_(b, attr_value(b, l + 1)) && r;
    r = p && attr_4(b, l + 1) && r;
    exit_section_(b, l, m, BNF_ATTR, r, p, attr_recover_until_parser_);
    return r || p;
  }

  // attr_pattern?
  private static boolean attr_1(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "attr_1")) return false;
    attr_pattern(b, l + 1);
    return true;
  }

  // ';'?
  private static boolean attr_4(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "attr_4")) return false;
    consumeToken(b, BNF_SEMICOLON);
    return true;
  }

  /* ********************************************************** */
  // '(' string ')'
  public static boolean attr_pattern(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "attr_pattern")) return false;
    if (!nextTokenIs(b, BNF_LEFT_PAREN)) return false;
    boolean r;
    Marker m = enter_section_(b);
    r = consumeToken(b, BNF_LEFT_PAREN);
    r = r && consumeToken(b, BNF_STRING);
    r = r && consumeToken(b, BNF_RIGHT_PAREN);
    exit_section_(b, m, BNF_ATTR_PATTERN, r);
    return r;
  }

  /* ********************************************************** */
  // !'}'
  static boolean attr_recover_until(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "attr_recover_until")) return false;
    boolean r;
    Marker m = enter_section_(b, l, _NOT_, null);
    r = !consumeToken(b, BNF_RIGHT_BRACE);
    exit_section_(b, l, m, null, r, false, null);
    return r;
  }

  /* ********************************************************** */
  // (reference_or_token | literal_expression) !'='
  public static boolean attr_value(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "attr_value")) return false;
    boolean r;
    Marker m = enter_section_(b, l, _NONE_, "<attr value>");
    r = attr_value_0(b, l + 1);
    r = r && attr_value_1(b, l + 1);
    exit_section_(b, l, m, BNF_ATTR_VALUE, r, false, null);
    return r;
  }

  // reference_or_token | literal_expression
  private static boolean attr_value_0(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "attr_value_0")) return false;
    boolean r;
    Marker m = enter_section_(b);
    r = reference_or_token(b, l + 1);
    if (!r) r = literal_expression(b, l + 1);
    exit_section_(b, m, null, r);
    return r;
  }

  // !'='
  private static boolean attr_value_1(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "attr_value_1")) return false;
    boolean r;
    Marker m = enter_section_(b, l, _NOT_, null);
    r = !consumeToken(b, BNF_OP_EQ);
    exit_section_(b, l, m, null, r, false, null);
    return r;
  }

  /* ********************************************************** */
  // '{' attr* '}'
  public static boolean attrs(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "attrs")) return false;
    if (!nextTokenIs(b, BNF_LEFT_BRACE)) return false;
    boolean r, p;
    Marker m = enter_section_(b, l, _NONE_, null);
    r = consumeToken(b, BNF_LEFT_BRACE);
    p = r; // pin = 1
    r = r && report_error_(b, attrs_1(b, l + 1));
    r = p && consumeToken(b, BNF_RIGHT_BRACE) && r;
    exit_section_(b, l, m, BNF_ATTRS, r, p, null);
    return r || p;
  }

  // attr*
  private static boolean attrs_1(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "attrs_1")) return false;
    int c = current_position_(b);
    while (true) {
      if (!attr(b, l + 1)) break;
      if (!empty_element_parsed_guard_(b, "attrs_1", c)) break;
      c = current_position_(b);
    }
    return true;
  }

  /* ********************************************************** */
  // '{' sequence ('|' sequence)* '}' | sequence choice_tail*
  public static boolean choice(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "choice")) return false;
    boolean r;
    Marker m = enter_section_(b, l, _COLLAPSE_, "<choice>");
    r = choice_0(b, l + 1);
    if (!r) r = choice_1(b, l + 1);
    exit_section_(b, l, m, BNF_CHOICE, r, false, null);
    return r;
  }

  // '{' sequence ('|' sequence)* '}'
  private static boolean choice_0(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "choice_0")) return false;
    boolean r;
    Marker m = enter_section_(b);
    r = consumeToken(b, BNF_LEFT_BRACE);
    r = r && sequence(b, l + 1);
    r = r && choice_0_2(b, l + 1);
    r = r && consumeToken(b, BNF_RIGHT_BRACE);
    exit_section_(b, m, null, r);
    return r;
  }

  // ('|' sequence)*
  private static boolean choice_0_2(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "choice_0_2")) return false;
    int c = current_position_(b);
    while (true) {
      if (!choice_0_2_0(b, l + 1)) break;
      if (!empty_element_parsed_guard_(b, "choice_0_2", c)) break;
      c = current_position_(b);
    }
    return true;
  }

  // '|' sequence
  private static boolean choice_0_2_0(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "choice_0_2_0")) return false;
    boolean r;
    Marker m = enter_section_(b);
    r = consumeToken(b, BNF_OP_OR);
    r = r && sequence(b, l + 1);
    exit_section_(b, m, null, r);
    return r;
  }

  // sequence choice_tail*
  private static boolean choice_1(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "choice_1")) return false;
    boolean r;
    Marker m = enter_section_(b);
    r = sequence(b, l + 1);
    r = r && choice_1_1(b, l + 1);
    exit_section_(b, m, null, r);
    return r;
  }

  // choice_tail*
  private static boolean choice_1_1(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "choice_1_1")) return false;
    int c = current_position_(b);
    while (true) {
      if (!choice_tail(b, l + 1)) break;
      if (!empty_element_parsed_guard_(b, "choice_1_1", c)) break;
      c = current_position_(b);
    }
    return true;
  }

  /* ********************************************************** */
  // '|' sequence
  static boolean choice_tail(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "choice_tail")) return false;
    if (!nextTokenIs(b, BNF_OP_OR)) return false;
    boolean r, p;
    Marker m = enter_section_(b, l, _NONE_, null);
    r = consumeToken(b, BNF_OP_OR);
    p = r; // pin = 1
    r = r && sequence(b, l + 1);
    exit_section_(b, l, m, null, r, p, null);
    return r || p;
  }

  /* ********************************************************** */
  // choice?
  public static boolean expression(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "expression")) return false;
    Marker m = enter_section_(b, l, _COLLAPSE_, "<expression>");
    choice(b, l + 1);
    exit_section_(b, l, m, BNF_EXPRESSION, true, false, null);
    return true;
  }

  /* ********************************************************** */
  // (attrs | rule) *
  static boolean grammar(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "grammar")) return false;
    int c = current_position_(b);
    while (true) {
      if (!grammar_0(b, l + 1)) break;
      if (!empty_element_parsed_guard_(b, "grammar", c)) break;
      c = current_position_(b);
    }
    return true;
  }

  // attrs | rule
  private static boolean grammar_0(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "grammar_0")) return false;
    boolean r;
    Marker m = enter_section_(b);
    r = attrs(b, l + 1);
    if (!r) r = rule(b, l + 1);
    exit_section_(b, m, null, r);
    return r;
  }

  /* ********************************************************** */
  // string_literal_expression | number
  public static boolean literal_expression(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "literal_expression")) return false;
    if (!nextTokenIs(b, "<literal expression>", BNF_NUMBER, BNF_STRING)) return false;
    boolean r;
    Marker m = enter_section_(b, l, _COLLAPSE_, "<literal expression>");
    r = string_literal_expression(b, l + 1);
    if (!r) r = consumeToken(b, BNF_NUMBER);
    exit_section_(b, l, m, BNF_LITERAL_EXPRESSION, r, false, null);
    return r;
  }

  /* ********************************************************** */
  // 'private' | 'external' | 'wrapped'
  public static boolean modifier(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "modifier")) return false;
    boolean r;
    Marker m = enter_section_(b, l, _NONE_, "<modifier>");
    r = consumeToken(b, "private");
    if (!r) r = consumeToken(b, "external");
    if (!r) r = consumeToken(b, "wrapped");
    exit_section_(b, l, m, BNF_MODIFIER, r, false, null);
    return r;
  }

  /* ********************************************************** */
  // quantified | predicate
  static boolean option(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "option")) return false;
    boolean r;
    Marker m = enter_section_(b);
    r = quantified(b, l + 1);
    if (!r) r = predicate(b, l + 1);
    exit_section_(b, m, null, r);
    return r;
  }

  /* ********************************************************** */
  // '(' expression ')'
  public static boolean paren_expression(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "paren_expression")) return false;
    if (!nextTokenIs(b, BNF_LEFT_PAREN)) return false;
    boolean r, p;
    Marker m = enter_section_(b, l, _NONE_, null);
    r = consumeToken(b, BNF_LEFT_PAREN);
    p = r; // pin = 1
    r = r && report_error_(b, expression(b, l + 1));
    r = p && consumeToken(b, BNF_RIGHT_PAREN) && r;
    exit_section_(b, l, m, BNF_PAREN_EXPRESSION, r, p, null);
    return r || p;
  }

  /* ********************************************************** */
  // predicate_sign  simple
  public static boolean predicate(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "predicate")) return false;
    if (!nextTokenIs(b, "<predicate>", BNF_OP_NOT, BNF_OP_AND)) return false;
    boolean r;
    Marker m = enter_section_(b, l, _NONE_, "<predicate>");
    r = predicate_sign(b, l + 1);
    r = r && simple(b, l + 1);
    exit_section_(b, l, m, BNF_PREDICATE, r, false, null);
    return r;
  }

  /* ********************************************************** */
  // '&' | '!'
  public static boolean predicate_sign(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "predicate_sign")) return false;
    if (!nextTokenIs(b, "<predicate sign>", BNF_OP_NOT, BNF_OP_AND)) return false;
    boolean r;
    Marker m = enter_section_(b, l, _NONE_, "<predicate sign>");
    r = consumeToken(b, BNF_OP_AND);
    if (!r) r = consumeToken(b, BNF_OP_NOT);
    exit_section_(b, l, m, BNF_PREDICATE_SIGN, r, false, null);
    return r;
  }

  /* ********************************************************** */
  // '[' expression ']' | simple quantifier?
  public static boolean quantified(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "quantified")) return false;
    boolean r;
    Marker m = enter_section_(b, l, _COLLAPSE_, "<quantified>");
    r = quantified_0(b, l + 1);
    if (!r) r = quantified_1(b, l + 1);
    exit_section_(b, l, m, BNF_QUANTIFIED, r, false, null);
    return r;
  }

  // '[' expression ']'
  private static boolean quantified_0(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "quantified_0")) return false;
    boolean r;
    Marker m = enter_section_(b);
    r = consumeToken(b, BNF_LEFT_BRACKET);
    r = r && expression(b, l + 1);
    r = r && consumeToken(b, BNF_RIGHT_BRACKET);
    exit_section_(b, m, null, r);
    return r;
  }

  // simple quantifier?
  private static boolean quantified_1(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "quantified_1")) return false;
    boolean r;
    Marker m = enter_section_(b);
    r = simple(b, l + 1);
    r = r && quantified_1_1(b, l + 1);
    exit_section_(b, m, null, r);
    return r;
  }

  // quantifier?
  private static boolean quantified_1_1(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "quantified_1_1")) return false;
    quantifier(b, l + 1);
    return true;
  }

  /* ********************************************************** */
  // '?' | '+' | '*'
  public static boolean quantifier(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "quantifier")) return false;
    boolean r;
    Marker m = enter_section_(b, l, _NONE_, "<quantifier>");
    r = consumeToken(b, BNF_OP_OPT);
    if (!r) r = consumeToken(b, BNF_OP_ONEMORE);
    if (!r) r = consumeToken(b, BNF_OP_ZEROMORE);
    exit_section_(b, l, m, BNF_QUANTIFIER, r, false, null);
    return r;
  }

  /* ********************************************************** */
  // id
  public static boolean reference_or_token(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "reference_or_token")) return false;
    if (!nextTokenIs(b, BNF_ID)) return false;
    boolean r;
    Marker m = enter_section_(b);
    r = consumeToken(b, BNF_ID);
    exit_section_(b, m, BNF_REFERENCE_OR_TOKEN, r);
    return r;
  }

  /* ********************************************************** */
  // modifier* id '::=' expression attrs? ';'?
  public static boolean rule(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "rule")) return false;
    boolean r, p;
    Marker m = enter_section_(b, l, _NONE_, "<rule>");
    r = rule_0(b, l + 1);
    r = r && consumeToken(b, BNF_ID);
    r = r && consumeToken(b, BNF_OP_IS);
    p = r; // pin = 3
    r = r && report_error_(b, expression(b, l + 1));
    r = p && report_error_(b, rule_4(b, l + 1)) && r;
    r = p && rule_5(b, l + 1) && r;
    exit_section_(b, l, m, BNF_RULE, r, p, rule_recover_until_parser_);
    return r || p;
  }

  // modifier*
  private static boolean rule_0(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "rule_0")) return false;
    int c = current_position_(b);
    while (true) {
      if (!modifier(b, l + 1)) break;
      if (!empty_element_parsed_guard_(b, "rule_0", c)) break;
      c = current_position_(b);
    }
    return true;
  }

  // attrs?
  private static boolean rule_4(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "rule_4")) return false;
    attrs(b, l + 1);
    return true;
  }

  // ';'?
  private static boolean rule_5(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "rule_5")) return false;
    consumeToken(b, BNF_SEMICOLON);
    return true;
  }

  /* ********************************************************** */
  // !'{'
  static boolean rule_recover_until(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "rule_recover_until")) return false;
    boolean r;
    Marker m = enter_section_(b, l, _NOT_, null);
    r = !consumeToken(b, BNF_LEFT_BRACE);
    exit_section_(b, l, m, null, r, false, null);
    return r;
  }

  /* ********************************************************** */
  // option +
  public static boolean sequence(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "sequence")) return false;
    boolean r;
    Marker m = enter_section_(b, l, _COLLAPSE_, "<sequence>");
    r = option(b, l + 1);
    int c = current_position_(b);
    while (r) {
      if (!option(b, l + 1)) break;
      if (!empty_element_parsed_guard_(b, "sequence", c)) break;
      c = current_position_(b);
    }
    exit_section_(b, l, m, BNF_SEQUENCE, r, false, null);
    return r;
  }

  /* ********************************************************** */
  // !(modifier* id '::=' ) reference_or_token | literal_expression | paren_expression
  static boolean simple(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "simple")) return false;
    boolean r;
    Marker m = enter_section_(b);
    r = simple_0(b, l + 1);
    if (!r) r = literal_expression(b, l + 1);
    if (!r) r = paren_expression(b, l + 1);
    exit_section_(b, m, null, r);
    return r;
  }

  // !(modifier* id '::=' ) reference_or_token
  private static boolean simple_0(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "simple_0")) return false;
    boolean r;
    Marker m = enter_section_(b);
    r = simple_0_0(b, l + 1);
    r = r && reference_or_token(b, l + 1);
    exit_section_(b, m, null, r);
    return r;
  }

  // !(modifier* id '::=' )
  private static boolean simple_0_0(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "simple_0_0")) return false;
    boolean r;
    Marker m = enter_section_(b, l, _NOT_, null);
    r = !simple_0_0_0(b, l + 1);
    exit_section_(b, l, m, null, r, false, null);
    return r;
  }

  // modifier* id '::='
  private static boolean simple_0_0_0(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "simple_0_0_0")) return false;
    boolean r;
    Marker m = enter_section_(b);
    r = simple_0_0_0_0(b, l + 1);
    r = r && consumeToken(b, BNF_ID);
    r = r && consumeToken(b, BNF_OP_IS);
    exit_section_(b, m, null, r);
    return r;
  }

  // modifier*
  private static boolean simple_0_0_0_0(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "simple_0_0_0_0")) return false;
    int c = current_position_(b);
    while (true) {
      if (!modifier(b, l + 1)) break;
      if (!empty_element_parsed_guard_(b, "simple_0_0_0_0", c)) break;
      c = current_position_(b);
    }
    return true;
  }

  /* ********************************************************** */
  // string
  public static boolean string_literal_expression(PsiBuilder b, int l) {
    if (!recursion_guard_(b, l, "string_literal_expression")) return false;
    if (!nextTokenIs(b, BNF_STRING)) return false;
    boolean r;
    Marker m = enter_section_(b);
    r = consumeToken(b, BNF_STRING);
    exit_section_(b, m, BNF_STRING_LITERAL_EXPRESSION, r);
    return r;
  }

  final static Parser attr_recover_until_parser_ = new Parser() {
    public boolean parse(PsiBuilder b, int l) {
      return attr_recover_until(b, l + 1);
    }
  };
  final static Parser rule_recover_until_parser_ = new Parser() {
    public boolean parse(PsiBuilder b, int l) {
      return rule_recover_until(b, l + 1);
    }
  };
}
```
**Example:sql** 
```sql
CREATE KEYSPACE videodb WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

use videodb;

// Basic entity table
// Object mapping ?
CREATE TABLE users (
   username varchar,
   firstname varchar,
   lastname varchar,
   email varchar,
   password varchar,
   created_date timestamp,
   total_credits int,
   credit_change_date timeuuid,
   PRIMARY KEY (username)
);

// One-to-many entity table
CREATE TABLE videos (
   videoid uuid,
   videoname varchar,
   username varchar,
   description varchar, 
   tags list<varchar>,
   upload_date timestamp,
   PRIMARY KEY (videoid)
);

// One-to-many from the user point of view
// Also know as a lookup table
CREATE TABLE username_video_index (
   username varchar,
   videoid uuid,
   upload_date timestamp,
   videoname varchar,
   PRIMARY KEY (username, videoid)
);

// Counter table
CREATE TABLE video_rating (
   videoid uuid,
   rating_counter counter,
   rating_total counter,
   PRIMARY KEY (videoid)
);

// Creating index tables for tab keywords
CREATE TABLE tag_index (
   tag varchar, 
   videoid uuid,
   timestamp timestamp,
   PRIMARY KEY (tag, videoid)
);

// Comments as a many-to-many 
// Looking from the video side to many users
CREATE TABLE comments_by_video (
   videoid uuid,
   username varchar,
   comment_ts timestamp,
   comment varchar,
   PRIMARY KEY (videoid,comment_ts,username)
) WITH CLUSTERING ORDER BY (comment_ts DESC, username ASC);

// looking from the user side to many videos
CREATE TABLE comments_by_user (
   username varchar,
   videoid uuid,
   comment_ts timestamp,
   comment varchar,
   PRIMARY KEY (username,comment_ts,videoid)
) WITH CLUSTERING ORDER BY (comment_ts DESC, videoid ASC);


// Time series wide row with reverse comparator
CREATE TABLE video_event (
   videoid uuid,
   username varchar,
   event varchar,
   event_timestamp timeuuid,
   video_timestamp bigint,
   PRIMARY KEY ((videoid,username), event_timestamp,event)
) WITH CLUSTERING ORDER BY (event_timestamp DESC,event ASC);
```
**Example:abap 219 rows** 
```abap
*/**
* The MIT License (MIT)
* Copyright (c) 2012 René van Mil
* 
* Permission is hereby granted, free of charge, to any person obtaining
* a copy of this software and associated documentation files (the
* "Software"), to deal in the Software without restriction, including
* without limitation the rights to use, copy, modify, merge, publish,
* distribute, sublicense, and/or sell copies of the Software, and to
* permit persons to whom the Software is furnished to do so, subject to
* the following conditions:
* 
* The above copyright notice and this permission notice shall be
* included in all copies or substantial portions of the Software.
* 
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
* EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
* MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
* IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
* CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
* TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
* SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

*----------------------------------------------------------------------*
*       CLASS CL_CSV_PARSER DEFINITION
*----------------------------------------------------------------------*
*
*----------------------------------------------------------------------*
class cl_csv_parser definition
  public
  inheriting from cl_object
  final
  create public .

  public section.
*"* public components of class CL_CSV_PARSER
*"* do not include other source files here!!!

    type-pools abap .
    methods constructor
      importing
        !delegate type ref to if_csv_parser_delegate
        !csvstring type string
        !separator type c
        !skip_first_line type abap_bool .
    methods parse
      raising
        cx_csv_parse_error .
  protected section.
*"* protected components of class CL_CSV_PARSER
*"* do not include other source files here!!!
  private section.
*"* private components of class CL_CSV_PARSER
*"* do not include other source files here!!!

    constants _textindicator type c value '"'.              "#EC NOTEXT
    data _delegate type ref to if_csv_parser_delegate .
    data _csvstring type string .
    data _separator type c .
    type-pools abap .
    data _skip_first_line type abap_bool .

    methods _lines
      returning
        value(returning) type stringtab .
    methods _parse_line
      importing
        !line type string
      returning
        value(returning) type stringtab
      raising
        cx_csv_parse_error .
endclass.                    "CL_CSV_PARSER DEFINITION



*----------------------------------------------------------------------*
*       CLASS CL_CSV_PARSER IMPLEMENTATION
*----------------------------------------------------------------------*
*
*----------------------------------------------------------------------*
class cl_csv_parser implementation.


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Public Method CL_CSV_PARSER->CONSTRUCTOR
* +-------------------------------------------------------------------------------------------------+
* | [--->] DELEGATE                       TYPE REF TO IF_CSV_PARSER_DELEGATE
* | [--->] CSVSTRING                      TYPE        STRING
* | [--->] SEPARATOR                      TYPE        C
* | [--->] SKIP_FIRST_LINE                TYPE        ABAP_BOOL
* +--------------------------------------------------------------------------------------</SIGNATURE>
  method constructor.
    super->constructor( ).
    _delegate = delegate.
    _csvstring = csvstring.
    _separator = separator.
    _skip_first_line = skip_first_line.
  endmethod.                    "constructor


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Public Method CL_CSV_PARSER->PARSE
* +-------------------------------------------------------------------------------------------------+
* | [!CX!] CX_CSV_PARSE_ERROR
* +--------------------------------------------------------------------------------------</SIGNATURE>
  method parse.
    data msg type string.
    if _csvstring is initial.
      message e002(csv) into msg.
      raise exception type cx_csv_parse_error
        exporting
          message = msg.
    endif.

    " Get the lines
    data is_first_line type abap_bool value abap_true.
    data lines type standard table of string.
    lines = _lines( ).
    field-symbols <line> type string.
    loop at lines assigning <line>.
      " Should we skip the first line?
      if _skip_first_line = abap_true and is_first_line = abap_true.
        is_first_line = abap_false.
        continue.
      endif.
      " Parse the line
      data values type standard table of string.
      values = _parse_line( <line> ).
      " Send values to delegate
      _delegate->values_found( values ).
    endloop.
  endmethod.                    "parse


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Private Method CL_CSV_PARSER->_LINES
* +-------------------------------------------------------------------------------------------------+
* | [<-()] RETURNING                      TYPE        STRINGTAB
* +--------------------------------------------------------------------------------------</SIGNATURE>
  method _lines.
    split _csvstring at cl_abap_char_utilities=>cr_lf into table returning.
  endmethod.                    "_lines


* <SIGNATURE>---------------------------------------------------------------------------------------+
* | Instance Private Method CL_CSV_PARSER->_PARSE_LINE
* +-------------------------------------------------------------------------------------------------+
* | [--->] LINE                           TYPE        STRING
* | [<-()] RETURNING                      TYPE        STRINGTAB
* | [!CX!] CX_CSV_PARSE_ERROR
* +--------------------------------------------------------------------------------------</SIGNATURE>
  method _parse_line.
    data msg type string.

    data csvvalue type string.
    data csvvalues type standard table of string.

    data char type c.
    data pos type i value 0.
    data len type i.
    len = strlen( line ).
    while pos < len.
      char = line+pos(1).
      if char <> _separator.
        if char = _textindicator.
          data text_ended type abap_bool.
          text_ended = abap_false.
          while text_ended = abap_false.
            pos = pos + 1.
            if pos < len.
              char = line+pos(1).
              if char = _textindicator.
                text_ended = abap_true.
              else.
                if char is initial. " Space
                  concatenate csvvalue ` ` into csvvalue.
                else.
                  concatenate csvvalue char into csvvalue.
                endif.
              endif.
            else.
              " Reached the end of the line while inside a text value
              " This indicates an error in the CSV formatting
              text_ended = abap_true.
              message e003(csv) into msg.
              raise exception type cx_csv_parse_error
                exporting
                  message = msg.
            endif.
          endwhile.
          " Check if next character is a separator, otherwise the CSV formatting is incorrect
          data nextpos type i.
          nextpos = pos + 1.
          if nextpos < len and line+nextpos(1) <> _separator.
            message e003(csv) into msg.
            raise exception type cx_csv_parse_error
              exporting
                message = msg.
          endif.
        else.
          if char is initial. " Space
            concatenate csvvalue ` ` into csvvalue.
          else.
            concatenate csvvalue char into csvvalue.
          endif.
        endif.
      else.
        append csvvalue to csvvalues.
        clear csvvalue.
      endif.
      pos = pos + 1.
    endwhile.
    append csvvalue to csvvalues. " Don't forget the last value

    returning = csvvalues.
  endmethod.                    "_parse_line
endclass.                    "CL_CSV_PARSER IMPLEMENTATION
```
**Example:html** 
```html
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta http-equiv="Content-Type" content="text/xhtml;charset=UTF-8"/>
<title>Related Pages</title>
<link href="qt.css" rel="stylesheet" type="text/css"/>
</head>
<body>
<div class=header>
<a class=headerLink  href="index.html">Main Page</a> &middot;
<a class=headerLink  href="classoverview.html">Class Overview</a> &middot;
<a class=headerLink  href="hierarchy.html">Hierarchy</a> &middot;
<a class=headerLink  href="annotated.html">All Classes</a>
</div>
<!-- Generated by Doxygen 1.8.1.2 -->
</div><!-- top -->
<div class="header">
  <div class="headertitle">
<div class="title">Related Pages</div>  </div>
</div><!--header-->
<div class="contents">
<div class="textblock">Here is a list of all related documentation pages:</div><div class="directory">
<table class="directory">
<tr id="row_0_" class="even"><td class="entry"><img src="ftv2node.png" alt="o" width="16" height="22" /><a class="el" href="classoverview.html" target="_self">Class Overview</a></td><td class="desc"></td></tr>
<tr id="row_1_"><td class="entry"><img src="ftv2lastnode.png" alt="\" width="16" height="22" /><a class="el" href="thelayoutsystem.html" target="_self">The Layout System</a></td><td class="desc"></td></tr>
</table>
</div><!-- directory -->
</div><!-- contents -->
<div class="footer" />Generated with <a href="http://www.doxygen.org/index.html">Doxygen</a> 1.8.1.2</div>
</body>
</html>
```
**Example:json** 
```json
{
     "firstName": "John",
     "lastName" : "Smith",
     "age"      : 25,
     "address"  :
     {
         "streetAddress": "21 2nd Street",
         "city"         : "New York",
         "state"        : "NY",
         "postalCode"   : "10021"
     },
     "phoneNumber":
     [
         {
           "type"  : "home",
           "number": "212 555-1234"
         },
         {
           "type"  : "fax",
           "number": "646 555-4567"
         }
     ]
 }
 {
        "name":"Product",
        "properties":
        {
                "id":
                {
                        "type":"number",
                        "description":"Product identifier",
                        "required":true
                },
                "name":
                {
                        "type":"string",
                        "description":"Name of the product",
                        "required":true
                },
                "price":
                {
                        "type":"number",
                        "minimum":0,
                        "required":true
                },
                "tags":
                {
                        "type":"array",
                        "items":
                        {
                                "type":"string"
                        }
                },
                "stock":
                {
                        "type":"object",
                        "properties":
                        {
                                "warehouse":
                                {
                                        "type":"number"
                                },
                                "retail":
                                {
                                        "type":"number"
                                }
                        }
                }
        }
}
{
        "id": 1,
        "name": "Foo",
        "price": 123,
        "tags": ["Bar","Eek"],
        "stock": { "warehouse":300, "retail":20 }
}
```
