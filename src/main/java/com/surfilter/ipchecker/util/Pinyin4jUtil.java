package com.surfilter.ipchecker.util;

import net.sourceforge.pinyin4j.PinyinHelper;
import net.sourceforge.pinyin4j.format.HanyuPinyinCaseType;
import net.sourceforge.pinyin4j.format.HanyuPinyinOutputFormat;
import net.sourceforge.pinyin4j.format.HanyuPinyinToneType;
import net.sourceforge.pinyin4j.format.HanyuPinyinVCharType;
import net.sourceforge.pinyin4j.format.exception.BadHanyuPinyinOutputFormatCombination;
import org.apache.log4j.Logger;

public class Pinyin4jUtil {

    private static final Logger log = Logger.getLogger(Pinyin4jUtil.class);

    /**
     * 将汉字转化为拼音
     * @param hanyu
     * @return
     */
    public static String toHanYuPinyinString(String hanyu) {
        HanyuPinyinOutputFormat format = new HanyuPinyinOutputFormat();
        format.setToneType(HanyuPinyinToneType.WITHOUT_TONE);
        format.setCaseType(HanyuPinyinCaseType.LOWERCASE);
        format.setVCharType(HanyuPinyinVCharType.WITH_U_UNICODE);
        try {
            String pinyinString = PinyinHelper.toHanYuPinyinString(hanyu, format, "", false);
            return pinyinString;
        } catch (BadHanyuPinyinOutputFormatCombination badHanyuPinyinOutputFormatCombination) {
            log.error(badHanyuPinyinOutputFormatCombination);
        }
        return hanyu;
    }
}
