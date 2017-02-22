package com.conveyal.datatools.editor.utils.tags;

//import play.templates.JavaExtensions;

import java.text.DecimalFormat;

public class TimeExtensions {

    public static String ccyAmount(Number number) {
         Integer min = (int)Math.floor(number.doubleValue()/new Double(60));
         Integer sec = (int)number.intValue() % 60;

         DecimalFormat twoDigits = new DecimalFormat("00");

         return twoDigits.format(min) + ":" + twoDigits.format(sec);
      }

    public static String hmsFormat(Number number) {
        if(number == null)
            return "00:00:00";

         Integer hour = (int)Math.floor(number.doubleValue()/new Double(60 * 60));
         Integer min = (int)Math.floor(number.doubleValue()/new Double(60)) % 60;
         Integer sec = (int)number.intValue() % 60;

         DecimalFormat twoDigits = new DecimalFormat("00");

         return twoDigits.format(hour) + ":" + twoDigits.format(min) + ":" + twoDigits.format(sec);
      }

}
