/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.util.redaction;

import static org.apache.geode.internal.util.redaction.ParserRegex.getPattern;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.regex.Pattern;

import org.junit.Test;

import org.apache.geode.internal.util.redaction.ParserRegex.Group;

public class ParserRegexTest {

  @Test
  public void capturesOption() {
    var input = "--option=argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument");
  }

  @Test
  public void capturesOptionWhenPrefixIsHyphenD() {
    var input = "-Doption=argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("-D");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument");
  }

  @Test
  public void capturesOptionWhenPrefixIsHyphensJD() {
    var input = "--J=-Doption=argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--J=-D");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument");
  }

  @Test
  public void capturesOptionWhenAssignIsSpace() {
    var input = "--option argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo(" ");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument");
  }

  @Test
  public void capturesOptionWhenAssignIsSpaceEquals() {
    var input = "--option =argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo(" =");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument");
  }

  @Test
  public void capturesOptionWhenAssignIsEqualsSpace() {
    var input = "--option= argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("= ");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument");
  }

  @Test
  public void capturesOptionWhenAssignIsSpaceEqualsSpace() {
    var input = "--option = argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo(" = ");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument");
  }

  @Test
  public void capturesOptionWhenKeyContainsHyphens() {
    var input = "--this-is-the-option=argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("this-is-the-option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument");
  }

  @Test
  public void capturesOptionWhenValueContainsHyphens() {
    var input = "--option=this-is-the-argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("this-is-the-argument");
  }

  @Test
  public void capturesOptionWhenValueIsQuoted() {
    var input = "--option=\"argument\"";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("\"argument\"");
  }

  @Test
  public void capturesOptionWhenValueIsQuotedAndAssignIsSpace() {
    var input = "--option \"argument\"";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo(" ");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("\"argument\"");
  }

  @Test
  public void capturesOptionWhenAssignContainsTwoSpaces() {
    var input = "--option  argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("  ");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument");
  }

  @Test
  public void capturesOptionWhenAssignContainsManySpaces() {
    var input = "--option   argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("   ");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument");
  }

  @Test
  public void capturesOptionWhenValueBeginsWithHyphen() {
    var input = "--option=-argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("-argument");
  }

  @Test
  public void capturesOptionWhenValueBeginsWithHyphenAndAssignIsSpace() {
    var input = "--option -argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo(" ");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("-argument");
  }

  @Test
  public void capturesOptionWhenQuotedValueBeginsWithHyphen() {
    var input = "--option=\"-argument\"";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("\"-argument\"");
  }

  @Test
  public void capturesOptionWhenValueBeginsWithTwoHyphens() {
    var input = "--option=--argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("--argument");
  }

  @Test
  public void capturesFlag() {
    var input = "--flag";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("flag");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();
  }

  @Test
  public void capturesFlagWhenPrefixIsHyphenD() {
    var input = "-Dflag";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("-D");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("flag");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();
  }

  @Test
  public void capturesFlagWhenPrefixIsHyphensJD() {
    var input = "--J=-Dflag";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--J=-D");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("flag");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();
  }

  @Test
  public void capturesTwoFlags() {
    var input = "--option --argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("argument");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();
  }

  @Test
  public void capturesTwoFlagsWhenPrefixIsHyphenD() {
    var input = "-Doption -Dargument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("-D");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("-D");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("argument");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();
  }

  @Test
  public void capturesTwoFlagsWhenPrefixIsHyphensJD() {
    var input = "--J=-Doption --J=-Dargument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--J=-D");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--J=-D");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("argument");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();
  }

  @Test
  public void capturesOptionWhenQuotedValueBeginsWithTwoHyphens() {
    var input = "--option=\"--argument\"";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("\"--argument\"");
  }

  @Test
  public void capturesOptionWhenQuotedValueBeginsWithHyphenD() {
    var input = "--option=\"-Dargument\"";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("\"-Dargument\"");
  }

  @Test
  public void capturesOptionWhenQuotedValueBeginsWithHyphensJD() {
    var input = "--option=\"--J=-Dargument\"";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("\"--J=-Dargument\"");
  }

  @Test
  public void capturesOptionWhenValueBeginsWithManyHyphens() {
    var input = "--option=---argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("---argument");
  }

  @Test
  public void capturesTwoFlagsWhenValueBeginsWithManyHyphensAndAssignIsSpace() {
    var input = "--option ---argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("-argument");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();
  }

  @Test
  public void capturesOptionWhenQuotedValueBeginsWithManyHyphens() {
    var input = "--option=\"---argument\"";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("\"---argument\"");
  }

  @Test
  public void capturesOptionWhenValueBeginsWithHyphenD() {
    var input = "--option=-Dargument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("-Dargument");
  }

  @Test
  public void capturesOptionWhenValueBeginsWithHyphensJD() {
    var input = "--option=--J=-Dargument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("--J=-Dargument");
  }

  @Test
  public void capturesOptionWithPartialValueWhenValueContainsSpace() {
    var input = "--option=foo bar";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("foo");

    assertThat(matcher.find()).isFalse();
  }

  @Test
  public void capturesOptionWithPartialValueWhenValueContainsSpaceAndSingleHyphens() {
    var input = "--option=-foo -bar";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("-foo");

    assertThat(matcher.find()).isFalse();
  }

  @Test
  public void capturesOptionWhenQuotedValueContainsSpaceAndSingleHyphens() {
    var input = "--option=\"-foo -bar\"";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("\"-foo -bar\"");
  }

  @Test
  public void capturesOptionAndFlagWhenValueContainsSpaceAndDoubleHyphens() {
    var input = "--option=--foo --bar";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("--foo");

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("bar");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();
  }

  @Test
  public void capturesOptionWhenQuotedValueContainsSpaceAndDoubleHyphens() {
    var input = "--option=\"--foo --bar\"";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("\"--foo --bar\"");
  }

  @Test
  public void capturesOptionWhenKeyContainsUnderscores() {
    var input = "--this_is_the_option=argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("this_is_the_option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument");
  }

  @Test
  public void capturesOptionWhenValueContainsUnderscores() {
    var input = "--option=this_is_the_argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("this_is_the_argument");
  }

  @Test
  public void capturesOptionWhenValueBeginsWithUnderscore() {
    var input = "--option=_argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("_argument");
  }

  @Test
  public void capturesOptionWhenValueBeginsWithUnderscoreAndAssignIsSpace() {
    var input = "--option _argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo(" ");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("_argument");
  }

  @Test
  public void capturesOptionWhenValueBeginsWithManyUnderscores() {
    var input = "--option=___argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("___argument");
  }

  @Test
  public void capturesOptionWhenValueBeginsWithManyUnderscoresAndAssignIsSpace() {
    var input = "--option ___argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo(" ");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("___argument");
  }

  @Test
  public void capturesOptionWhenKeyContainsPeriods() {
    var input = "--this.is.the.option=argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("this.is.the.option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument");
  }

  @Test
  public void capturesOptionWhenValueContainsPeriods() {
    var input = "--option=this.is.the.argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("this.is.the.argument");
  }

  @Test
  public void capturesOptionWhenValueBeginsWithPeriod() {
    var input = "--option=.argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo(".argument");
  }

  @Test
  public void capturesOptionWhenValueBeginsWithPeriodAndAssignIsSpace() {
    var input = "--option .argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo(" ");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo(".argument");
  }

  @Test
  public void capturesOptionWhenValueBeginsWithManyPeriods() {
    var input = "--option=...argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("...argument");
  }

  @Test
  public void capturesOptionWhenValueBeginsWithManyPeriodsAndAssignIsSpace() {
    var input = "--option ...argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo(" ");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("...argument");
  }

  @Test
  public void doesNotMatchWhenPrefixIsSingleHyphen() {
    var input = "-option=argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isFalse();
  }

  @Test
  public void doesNotMatchWhenPrefixIsMissing() {
    var input = "option=argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isFalse();
  }

  @Test
  public void groupZeroCapturesFullInputWhenValid() {
    var input = "--option=argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(0)).isEqualTo(input);
  }

  @Test
  public void groupValuesHasSizeEqualToGroupCount() {
    var input = "--option=argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(Group.values()).hasSize(matcher.groupCount());
  }

  @Test
  public void groupPrefixCapturesHyphens() {
    var input = "--option=argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
  }

  @Test
  public void groupPrefixCapturesHyphenD() {
    var input = "-Doption=argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("-D");
  }

  @Test
  public void groupPrefixCapturesHyphensJD() {
    var input = "--J=-Doption=argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--J=-D");
  }

  @Test
  public void groupPrefixCapturesIsolatedHyphens() {
    var prefix = "--";
    var matcher = Pattern.compile(Group.PREFIX.getRegex()).matcher(prefix);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group()).isEqualTo(prefix);
  }

  @Test
  public void groupPrefixCapturesIsolatedHyphenD() {
    var prefix = "-D";
    var matcher = Pattern.compile(Group.PREFIX.getRegex()).matcher(prefix);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group()).isEqualTo(prefix);
  }

  @Test
  public void groupPrefixCapturesIsolatedHyphensJD() {
    var prefix = "--J=-D";
    var matcher = Pattern.compile(Group.PREFIX.getRegex()).matcher(prefix);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group()).isEqualTo(prefix);
  }

  @Test
  public void groupKeyCapturesKey() {
    var input = "--option=argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option");
  }

  @Test
  public void groupKeyCapturesIsolatedKey() {
    var option = "option";
    var matcher = Pattern.compile(Group.KEY.getRegex()).matcher(option);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group()).isEqualTo(option);
  }

  @Test
  public void groupAssignCapturesEquals() {
    var input = "--option=argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
  }

  @Test
  public void groupAssignCapturesSpace() {
    var input = "--option argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo(" ");
  }

  @Test
  public void groupAssignCapturesIsolatedEqualsSurroundedBySpaces() {
    var assignment = " = ";
    var matcher = Pattern.compile(Group.ASSIGN.getRegex()).matcher(assignment);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group()).isEqualTo(assignment);
  }

  @Test
  public void groupValueCapturesValue() {
    var input = "--option=argument";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.matches()).isTrue();
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument");
  }

  @Test
  public void groupValueCapturesIsolatedValue() {
    var argument = "argument";
    var matcher = Pattern.compile(Group.VALUE.getRegex()).matcher(argument);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group()).isEqualTo(argument);
  }

  @Test
  public void groupValueCapturesIsolatedValueStartingWithManyHyphens() {
    var argument = "---argument";
    var matcher = Pattern.compile(Group.VALUE.getRegex()).matcher(argument);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group()).isEqualTo(argument);
  }

  @Test
  public void groupValueDoesNotMatchIsolatedValueContainingSpaces() {
    var argument = "foo bar oi vey";
    var matcher = Pattern.compile(Group.VALUE.getRegex()).matcher(argument);

    assertThat(matcher.matches()).isFalse();
  }

  @Test
  public void groupValueDoesNotMatchIsolatedValueContainingDoubleHyphensAndSpaces() {
    var argument = "--foo --bar --oi --vey";
    var matcher = Pattern.compile(Group.VALUE.getRegex()).matcher(argument);

    assertThat(matcher.matches()).isFalse();
  }

  @Test
  public void groupValueCapturesIsolatedQuotedValueContainingDoubleHyphensAndSpaces() {
    var argument = "\"--foo --bar --oi --vey\"";
    var matcher = Pattern.compile(Group.VALUE.getRegex()).matcher(argument);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group()).isEqualTo(argument);
  }

  @Test
  public void groupValueCapturesIsolatedValueEndingWithHyphen() {
    var argument = "value-";
    var matcher = Pattern.compile(Group.VALUE.getRegex()).matcher(argument);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group()).isEqualTo(argument);
  }

  @Test
  public void groupValueCapturesIsolatedValueEndingWithQuote() {
    var argument = "value\"";
    var matcher = Pattern.compile(Group.VALUE.getRegex()).matcher(argument);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group()).isEqualTo(argument);
  }

  @Test
  public void groupValueCapturesIsolatedValueContainingSymbols() {
    var argument = "'v@lu!\"t";
    var matcher = Pattern.compile(Group.VALUE.getRegex()).matcher(argument);
    assertThat(matcher.matches()).isTrue();

    assertThat(matcher.group()).isEqualTo(argument);
  }

  @Test
  public void capturesMultipleOptions() {
    var input = "--option1=argument1 --option2=argument2";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option1");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument1");

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option2");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument2");
  }

  @Test
  public void capturesFlagAfterMultipleOptions() {
    var input = "--option1=argument1 --option2=argument2 --flag";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option1");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument1");

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option2");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("argument2");

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("flag");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();
  }

  @Test
  public void capturesMultipleOptionsAfterFlag() {
    var input = "--flag --option1=foo --option2=.";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("flag");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option1");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("foo");

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option2");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo(".");
  }

  @Test
  public void capturesMultipleOptionsSurroundingFlag() {
    var input = "--option1=foo --flag --option2=.";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option1");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("foo");

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("flag");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("option2");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo(".");
  }

  @Test
  public void capturesMultipleOptionsAfterCommand() {
    var input = "command --key=value --foo=bar";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("key");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("value");

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("foo");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("bar");
  }

  @Test
  public void capturesMultipleOptionsSurroundingFlagAfterCommand() {
    var input = "command --key=value --flag --foo=bar";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("key");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("value");

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("flag");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("foo");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("bar");
  }

  @Test
  public void capturesMultipleOptionsWithVariousPrefixes() {
    var input = "--key=value -Dflag --J=-Dfoo=bar";
    var matcher = getPattern().matcher(input);

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("key");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("value");

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("-D");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("flag");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isNull();
    assertThat(matcher.group(Group.VALUE.getIndex())).isNull();

    assertThat(matcher.find()).isTrue();
    assertThat(matcher.group(Group.PREFIX.getIndex())).isEqualTo("--J=-D");
    assertThat(matcher.group(Group.KEY.getIndex())).isEqualTo("foo");
    assertThat(matcher.group(Group.ASSIGN.getIndex())).isEqualTo("=");
    assertThat(matcher.group(Group.VALUE.getIndex())).isEqualTo("bar");
  }
}
