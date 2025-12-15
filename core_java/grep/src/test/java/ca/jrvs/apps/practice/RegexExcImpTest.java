package ca.jrvs.apps.practice;
import org.junit.Test;
//import org.junit.Test.*;
//import static org.junit.jupiter.api.Assertions.*;
import static org.junit.Assert.*;


public class RegexExcImpTest {
    private final RegexExcImp regexExc = new RegexExcImp();

    // --- matchJpeg() Tests ---
    @Test
    public void testMatchJpeg_ValidExtensions() {
        assertTrue(regexExc.matchJpeg("photo.jpg"));
        assertTrue(regexExc.matchJpeg("picture.JPEG"));
        assertTrue(regexExc.matchJpeg("image.JpG"));
    }

    @Test
    public void testMatchJpeg_InvalidExtensions() {
        assertFalse(regexExc.matchJpeg("photo.png"));
        assertFalse(regexExc.matchJpeg("file.txt"));
        assertFalse(regexExc.matchJpeg("jpgfile"));
        assertFalse(regexExc.matchJpeg(null));
    }

    // --- matchIp() Tests ---
    @Test
    public void testMatchIp_ValidIPs() {
        assertTrue(regexExc.matchIp("0.0.0.0"));
        assertTrue(regexExc.matchIp("255.255.255.255"));
        assertTrue(regexExc.matchIp("999.999.999.999"));
        assertTrue(regexExc.matchIp("192.168.1.1"));
    }

    @Test
    public void testMatchIp_InvalidIPs() {
        assertFalse(regexExc.matchIp("192.168.1"));
        assertFalse(regexExc.matchIp("192.168.1.256.1"));
        assertFalse(regexExc.matchIp("abc.def.ghi.jkl"));
        assertFalse(regexExc.matchIp(""));
        assertFalse(regexExc.matchIp(null));
    }

    // --- isEmptyLine() Tests ---
    @Test
    public void testIsEmptyLine_TrueCases() {
        assertTrue(regexExc.matchisEmptyLine(""));
        assertTrue(regexExc.matchisEmptyLine("   "));
        assertTrue(regexExc.matchisEmptyLine("\t"));
        assertTrue(regexExc.matchisEmptyLine("\n"));
    }

    @Test
    public void testIsEmptyLine_FalseCases() {
        assertFalse(regexExc.matchisEmptyLine("hello"));
        assertFalse(regexExc.matchisEmptyLine("  world "));
        assertFalse(regexExc.matchisEmptyLine(null));
    }
}
