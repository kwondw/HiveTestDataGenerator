package kwon.dongwook.model;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.ThreadLocalRandom;


public class DummyDataGenerator implements DataGenerator {

    private static Log LOG = LogFactory.getLog(DummyDataGenerator.class);

    private final static String alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private final static String numbers = "0123456789";
    private final static String symbols = "~!@#$%^&*()_+-?><;[]'";
    private static final SimpleDateFormat fullDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final SimpleDateFormat shortDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private int stringMax = 10;
    private int stringMin = 1;
    private static ThreadLocalRandom random = ThreadLocalRandom.current();
    private static Calendar calendar = Calendar.getInstance();
    private static int MAX_TRY_FOR_UNIQUE_VALUE = 100;
    private boolean negativeNumber = false;

    public DummyDataGenerator() {

    }

    public String generateValue(ColumnInfo columnInfo) {
        return generateValue(columnInfo.getType(), columnInfo.getObjectInspector());
    }

    public String generateValue(TypeInfo typeInfo, ObjectInspector objectInspector) {
        if(typeInfo.getCategory() == ObjectInspector.Category.PRIMITIVE) {
            return generatePrimitiveData((PrimitiveTypeInfo) typeInfo, objectInspector);
        } else {
            return generateNonPrimitiveData(typeInfo, objectInspector);
        }
    }

    private String generatePrimitiveData(PrimitiveTypeInfo type, ObjectInspector objectInspector) {
        switch (type.getPrimitiveCategory()) {
            case STRING:
                return generateStringData(stringMin, stringMax);
            case BOOLEAN:
                return generateBooleanData();
            case DATE:
                return generateDateData();
            case SHORT:
                return generateShort();
            case DOUBLE:
                return generateFloatData();
            case INT:
                return generateIntegerData();
            case LONG: // BIGINT
                return generateLongData();
            case DECIMAL:
                return generateDecimal((DecimalTypeInfo) type);
            case VARCHAR:
              return genereateVarChar(objectInspector);
            case VOID:
              return "";
            case CHAR:
            case BYTE:
              return generateByte();
            case FLOAT:
              return generateFloatData();
            case TIMESTAMP:
              return generateTimestamp();
            case BINARY:
              return  generateByteArray();
            case INTERVAL_YEAR_MONTH:
                return generateIntervalYearMonth();
            case INTERVAL_DAY_TIME:
                return generateIntervalDayTime();
        }
        return "NULL";
    }


    private String generateIntervalDayTime() {
        HiveIntervalDayTime interval = new HiveIntervalDayTime(randRange(1, 30), randRange(0, 23),
                randRange(1, 60), randRange(0, 60), randRange(0, 100));
        return interval.toString();
    }

    private String generateIntervalYearMonth() {
        return new HiveIntervalYearMonth(randRange(1910, 2015), randRange(1, 12)).toString();
    }

    private String generateDecimal(DecimalTypeInfo type) {
        int precision = type.getPrecision();
        int scale = type.getScale();
        double number = random.nextGaussian();
        BigDecimal bigDecimal = new BigDecimal(number);
        HiveDecimal decimal = HiveDecimal.enforcePrecisionScale(HiveDecimal.create(bigDecimal), precision, scale);
        return decimal.toString();
    }

    private String genereateVarChar(ObjectInspector objectInspector) {
      int max = ((WritableHiveVarcharObjectInspector) objectInspector).getMaxLength();
      return generateStringData(1, max);
    }

    private Date randomDate() {
        DummyDataGenerator.calendar.set(Calendar.YEAR, randRange(2000, 2016));
        DummyDataGenerator.calendar.set(Calendar.MONTH, randRange(0, 12));
        DummyDataGenerator.calendar.set(Calendar.DAY_OF_MONTH, randRange(1, 31));
        DummyDataGenerator.calendar.set(Calendar.HOUR, randRange(0, 24));
        DummyDataGenerator.calendar.set(Calendar.MINUTE, randRange(0, 60));
        DummyDataGenerator.calendar.set(Calendar.MILLISECOND, randRange(0, 100));
        return DummyDataGenerator.calendar.getTime();
    }

    private String generateTimestamp() {
      return fullDateFormat.format(randomDate());
    }

    private String generateDateData() {
        return shortDateFormat.format(randomDate());
    }

    private String generateFloatData() {
        return String.format("%.4f", ThreadLocalRandom.current().nextFloat());
    }

    private String generateIntegerData() {
        int number = negativeNumber ? random.nextInt() : random.nextInt(0, Integer.MAX_VALUE);
        return Integer.toString(number);
    }

    private String generateLongData() {
        long number = negativeNumber ? random.nextLong() : random.nextLong(0, Long.MAX_VALUE);
        number = (number / random.nextInt(1, (int)Math.min((long) Integer.MAX_VALUE, number)));
        return Long.toString(number);
    }

    private String generateShort() {
        int number = negativeNumber ? random.nextInt(Short.MIN_VALUE, Short.MAX_VALUE): random.nextInt(0, Short.MAX_VALUE);
        return Short.toString((short)number);
    }

    private String generateByte() {
        byte[] bs = new byte[1];
        random.nextBytes(bs);
        return Byte.toString(bs[0]);
    }

    private String generateByteArray() {
        int length = randRange(3, 10);
        byte[] ba = new byte[length];
        random.nextBytes(ba);
        char[] ch = new char[length];
        for (int i = 0; i < ba.length; i++) {
          ch[i] = (char)ba[i];
        }
        return new String(ch);
    }

    private String generateBooleanData() {
        return Boolean.toString(ThreadLocalRandom.current().nextBoolean());
    }

    private String generateStringData(int min, int max) {
        int size = randRange(min, max);
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < size; i++) {
            int index = randRange(0, alphabet.length() - 1);
            sb.append(alphabet.charAt(index));
        }
        return sb.toString();
    }

    public String[] uniqueValuesOf(ColumnInfo info, int count) {
        HashSet<String> unique = new HashSet<String>(count);
        String value = null;

        generate_values:
        for(int i = 0; i < count; i++) {
            int tryToBeUnique = 0;
            do {
                value = generateValue(info.getType(), info.getObjectInspector());
                tryToBeUnique++;
                if(tryToBeUnique > MAX_TRY_FOR_UNIQUE_VALUE) {
                    LOG.info("Tried " + tryToBeUnique
                        + " times to find unique values, but it can't have enough unique values,"
                        + "please check your range of randomness.\n"
                        + "Return with " + unique.size()
                        + " out of " + count + " requested");
                    break generate_values;
                }
            } while (unique.contains(value));
            unique.add(value);
        }
        return unique.toArray(new String[unique.size()]);
    }

    private String generateNonPrimitiveData(TypeInfo type, ObjectInspector objectInspector) {
        return "NON_PRIMITIVE";
    }

    public static int randRange(int min, int max) {
        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }
}
