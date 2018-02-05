/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.metadata.model;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.kylin.metadata.datatype.DataType;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class ParameterDesc implements Serializable {

    public static class ExpressionParam {
        public static final String[] OPERATIONAL_SYMBOLS = new String[] { "+", "-", "*", "/" };
        public static final String ADDITION = "+";
        public static final String SUBSTRACTION = "-";
        public static final String MULTIPLICATION = "*";
        public static final String DIVISION = "/";

        private String operator;
        //operand is ordered or not will affect the result
        private boolean needOrderedPram;

        public ExpressionParam(String operator, boolean needOrderedPram) {
            this.operator = operator;
            this.needOrderedPram = needOrderedPram;
        }

        public String getOperator() {
            return operator;
        }

        public void setOperator(String operator) {
            this.operator = operator;
        }

        public static boolean isSupportOperation(String symbol) {
            return Lists.newArrayList(OPERATIONAL_SYMBOLS).contains(symbol);
        }

        public boolean isOrder() {
            return needOrderedPram;
        }

        public BigDecimal getValueOf(String[] operators) {
            if (operators.length < 3 || operators[1] == null || operators[2] == null)
                return new BigDecimal(0);
            switch (operator) {
            case ADDITION:
                return new BigDecimal(operators[1]).add(new BigDecimal(operators[2]));
            case SUBSTRACTION:
                return new BigDecimal(operators[1]).subtract(new BigDecimal(operators[2]));

            case MULTIPLICATION:
                return new BigDecimal(operators[1]).multiply(new BigDecimal(operators[2]));
            case DIVISION:
                return new BigDecimal(operators[1]).divide(new BigDecimal(operators[2]));
            default:
                throw new IllegalArgumentException();
            }
        }

        /**
         * derived return type of MathExpression Parameter according to the operator and the the types of operands
         */
        public static DataType deriveReturnType(String operator, String operandType1, String operandType2) {
            // for easily extend later, but now their return type are same
            switch (operator) {
            case ADDITION:
                return derivedMultiplyReturnType(operandType1, operandType2);
            case SUBSTRACTION:
                return derivedMultiplyReturnType(operandType1, operandType2);

            case MULTIPLICATION:
                return derivedMultiplyReturnType(operandType1, operandType2);
            case DIVISION:
                return derivedMultiplyReturnType(operandType1, operandType2);
            default:
                throw new IllegalArgumentException();
            }
        }

        private static DataType derivedMultiplyReturnType(String operandType1, String operandType2) {
            //get max precision data type
            DataType dataType1 = DataType.getType(operandType1);
            DataType dataType2 = DataType.getType(operandType2);
            if (dataType1.isDecimal() || dataType2.isDecimal()) {
                return new DataType("decimal", Math.max(dataType1.getPrecision(), dataType2.getPrecision()),
                        Math.max(dataType1.getScale(), dataType2.getScale()));
            }

            if (dataType1.isIntegerFamily() && dataType2.isIntegerFamily())
                return DataType.getType("bigint");

            return DataType.getType("double");
        }
    }

    public static ParameterDesc newInstance(Object... objs) {
        if (objs.length == 0)
            throw new IllegalArgumentException();

        ParameterDesc r = new ParameterDesc();

        Object obj = objs[0];
        if (obj instanceof TblColRef) {
            TblColRef col = (TblColRef) obj;
            if (col.getType() != null
                    && TblColRef.InnerDataTypeEnum.LITERAL.getDataType().equals(col.getType().getName())) {
                return convertToParameterList(col);
            }
            r.type = FunctionDesc.PARAMETER_TYPE_COLUMN;
            r.value = col.getIdentity();
            r.colRef = col;
        } else if (obj instanceof ExpressionParam) {
            r.type = FunctionDesc.PARAMETER_TYPE_MATH_EXPRESSION;
            r.expressionParam = (ExpressionParam) obj;
            r.value = ((ExpressionParam) obj).getOperator();
        } else {
            r.type = FunctionDesc.PARAMETER_TYPE_CONSTANT;
            r.value = (String) obj;
        }

        if (objs.length >= 2) {
            r.nextParameter = newInstance(Arrays.copyOfRange(objs, 1, objs.length));
        }
        return r;
    }

    @JsonProperty("type")
    private String type;
    @JsonProperty("value")
    private String value;

    @JsonProperty("next_parameter")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private ParameterDesc nextParameter;

    private TblColRef colRef = null;
    private List<TblColRef> allColRefsIncludingNexts = null;
    private Set<PlainParameter> plainParameters = null;

    private ExpressionParam expressionParam;

    // Lazy evaluation
    public Set<PlainParameter> getPlainParameters() {
        if (plainParameters == null) {
            plainParameters = PlainParameter.createFromParameterDesc(this);
        }
        return plainParameters;
    }

    public ParameterDesc getCopyOf() {
        ParameterDesc parameterDesc = new ParameterDesc();
        parameterDesc.setNextParameter(this.nextParameter);
        parameterDesc.setColRef(this.colRef);
        parameterDesc.setValue(this.value);
        parameterDesc.setType(this.type);
        return parameterDesc;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public byte[] getBytes() throws UnsupportedEncodingException {
        return value.getBytes("UTF-8");
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public TblColRef getColRef() {
        return colRef;
    }

    void setColRef(TblColRef colRef) {
        this.colRef = colRef;
    }

    public ExpressionParam getExpressionParam() {
        return expressionParam;
    }

    public List<TblColRef> getColRefs() {
        if (allColRefsIncludingNexts == null) {
            List<TblColRef> all = new ArrayList<>(2);
            ParameterDesc p = this;
            while (p != null) {
                if (p.isColumnType())
                    all.add(p.getColRef());

                p = p.nextParameter;
            }
            allColRefsIncludingNexts = all;
        }
        return allColRefsIncludingNexts;
    }

    public ParameterDesc getNextParameter() {
        return nextParameter;
    }

    public void setNextParameter(ParameterDesc nextParameter) {
        this.nextParameter = nextParameter;
    }

    public boolean isColumnType() {
        return FunctionDesc.PARAMETER_TYPE_COLUMN.equals(type);
    }

    public boolean isConstant() {
        return FunctionDesc.PARAMETER_TYPE_CONSTANT.equals(type.toLowerCase());
    }

    public boolean isMathExpressionType() {
        return FunctionDesc.PARAMETER_TYPE_MATH_EXPRESSION.equals(type);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        if (FunctionDesc.PARAMETER_TYPE_MATH_EXPRESSION.equals(this.getType())
                && !this.getExpressionParam().isOrder()) {
            return equalInArbitraryOrder(o);
        }

        ParameterDesc that = (ParameterDesc) o;

        if (type != null ? !type.equals(that.type) : that.type != null)
            return false;

        ParameterDesc p = this, q = that;
        for (; p != null && q != null; p = p.nextParameter, q = q.nextParameter) {
            if (p.isColumnType()) {
                if (q.isColumnType() == false)
                    return false;
                if (q.getColRef().equals(p.getColRef()) == false)
                    return false;
            } else {
                if (q.isColumnType() == true)
                    return false;
                if (p.value.equals(q.value) == false)
                    return false;
            }
        }

        return p == null && q == null;
    }

    public boolean equalInArbitraryOrder(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ParameterDesc that = (ParameterDesc) o;

        Set<PlainParameter> thisPlainParams = this.getPlainParameters();
        Set<PlainParameter> thatPlainParams = that.getPlainParameters();

        return thisPlainParams.containsAll(thatPlainParams) && thatPlainParams.containsAll(thisPlainParams);
    }

    private static ParameterDesc convertToParameterList(TblColRef col) {
        if (SqlStdOperatorTable.MULTIPLY.equals(col.getOperator())) {
            ExpressionParam param = new ExpressionParam(col.getOperator().getName(), false);
            return newInstance(param, col.getOpreand().get(0), col.getOpreand().get(1));
        } else {
            ParameterDesc r = new ParameterDesc();
            r.type = FunctionDesc.PARAMETER_TYPE_COLUMN;
            r.value = col.getIdentity();
            r.colRef = col;
            return r;
        }
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (colRef != null ? colRef.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        String thisStr = isColumnType() ? colRef.toString() : value;
        return nextParameter == null ? thisStr : thisStr + "," + nextParameter.toString();
    }

    /**
     * PlainParameter is created to present ParameterDesc in List style.
     * Compared to ParameterDesc its advantage is:
     * 1. easy to compare without considering order
     * 2. easy to compare one by one
     */
    private static class PlainParameter {
        private String type;
        private String value;
        private TblColRef colRef = null;

        private PlainParameter() {
        }

        public boolean isColumnType() {
            return FunctionDesc.PARAMETER_TYPE_COLUMN.equals(type);
        }

        static Set<PlainParameter> createFromParameterDesc(ParameterDesc parameterDesc) {
            Set<PlainParameter> result = Sets.newHashSet();
            ParameterDesc local = parameterDesc;
            while (local != null) {
                if (local.isColumnType()) {
                    result.add(createSingleColumnParameter(local));
                } else {
                    result.add(createSingleValueParameter(local));
                }
                local = local.nextParameter;
            }
            return result;
        }

        static PlainParameter createSingleValueParameter(ParameterDesc parameterDesc) {
            PlainParameter single = new PlainParameter();
            single.type = parameterDesc.type;
            single.value = parameterDesc.value;
            return single;
        }

        static PlainParameter createSingleColumnParameter(ParameterDesc parameterDesc) {
            PlainParameter single = new PlainParameter();
            single.type = parameterDesc.type;
            single.value = parameterDesc.value;
            single.colRef = parameterDesc.colRef;
            return single;
        }

        @Override
        public int hashCode() {
            int result = type != null ? type.hashCode() : 0;
            result = 31 * result + (colRef != null ? colRef.hashCode() : 0);
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            PlainParameter that = (PlainParameter) o;

            if (type != null ? !type.equals(that.type) : that.type != null)
                return false;

            if (this.isColumnType()) {
                if (!that.isColumnType())
                    return false;
                if (!this.colRef.equals(that.colRef)) {
                    return false;
                }
            } else {
                if (that.isColumnType())
                    return false;
                if (!this.value.equals(that.value))
                    return false;
            }

            return true;
        }
    }
}
