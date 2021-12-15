package com.nexr.udfs;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.StdLong;
import com.linkedin.transport.api.data.StdString;
import com.linkedin.transport.api.udf.StdUDF4;
import com.linkedin.transport.api.udf.TopLevelStdUDF;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Source Date String, Date String Pattern, lang (ko, en),
 * timezone과 같이 4개의 인자를 받아 Timezone이 적용된 Date String을 출력
 *
 */
public class UdfUnixTimestampWithTz extends StdUDF4<StdString, StdString, StdString, StdString, StdLong> implements TopLevelStdUDF {

    private StdLong convertedTimeStamp;

    @Override
    public List<String> getInputParameterSignatures() {
        return ImmutableList.of(
                "varchar",
                "varchar",
                "varchar",
                "varchar"
        );
    }

    @Override
    public String getOutputParameterSignature() {
        return "bigint";
    }

    @Override
    public void init(StdFactory stdFactory) {
        super.init(stdFactory);
    }

    @Override
    public String getFunctionName() {
        return "applyTomeZoneToUnixTime";
    }

    @Override
    public String getFunctionDescription() {
        return "Convert Source Date String into a Date String applied timezone and lang";
    }

    @Override
    public StdLong eval(StdString inputDateTime, StdString pattern, StdString lang, StdString timeZone) {
        if (inputDateTime == null) {
            return null;
        }

        if (pattern == null) {
            return null;
        }

        TimeZone tz = getTimeZone(timeZone);
        Locale locale = null;

        if (lang == null) {
            locale = Locale.KOREA;
        } else {
            locale = lang.get().equalsIgnoreCase("ko") ? Locale.KOREA : Locale.ENGLISH;
        }

        try {
            long timeStamp = parseTime(inputDateTime.get(), pattern.get(), locale, tz);
            return this.getStdFactory().createLong(timeStamp);
        } catch (ParseException e) {
           return null;
        }
    }

    private TimeZone getTimeZone(StdString userTimeZoneId) {
        if (userTimeZoneId == null) // Will use default timezone.
            return TimeZone.getDefault();

        /* If it doesn't find any matched timezone, The GMT timezone will return. */
        TimeZone searchedTimeZone = TimeZone.getTimeZone(userTimeZoneId.get());
        return searchedTimeZone.getID().equals(userTimeZoneId.get()) ? searchedTimeZone : TimeZone.getDefault();
    }

    public long parseTime(String timeString, String pattern, Locale locale, TimeZone timeZone) throws ParseException {
        if (!pattern.contains("y")) {
            pattern = pattern + "-yyyy";
            timeString = timeString + "-" + Calendar.getInstance().get(Calendar.YEAR);
        }

        SimpleDateFormat format = new SimpleDateFormat(pattern, locale);
        format.setTimeZone(timeZone);
        System.out.println("Format Time Zone : " + format.getTimeZone().getID());
        return format.parse(timeString).getTime();
    }

}
