{% docs race %}
### 🧬 Race

The **`race`** column categorizes individuals based on self-identified or observed racial or ethnic background.  
It follows a standardized ordinal coding system where lower numbers may indicate higher priority or prevalence  
in certain reporting contexts (e.g., federal systems such as FBI UCR or CDC demographic datasets).

**Code Reference Table**

| Code | Label | Description |
|------|--------|-------------|
| 1 | **Black** | Individuals who identify as Black or African American. |
| 2 | **White** | Individuals who identify as White or Caucasian. |
| 3 | **Unknown** | Race could not be determined or was not reported. |
| 4 | **Other** | Individuals not in standard categories (e.g., multiracial not specified). |
| 5 | **Asian** | Individuals who identify as Asian (including East, South, and Southeast Asian). |
| 6 | **Hispanic** | Individuals who identify as Hispanic or Latino (note: often treated as ethnicity). |
| 7 | **Native Hawaiian or Other Pacific Islander** | Individuals from Native Hawaiian, Samoan, Guamanian, or other Pacific Islander groups. |

---

#### 📝 Notes
- The numeric ordering (1 = Black, 2 = White, etc.) follows legacy public-sector systems and may not reflect population proportionality.  
- “Hispanic” is included as a race value despite being an ethnicity in modern OMB standards, due to operational usage.  
- Use with caution in analytical contexts; consider cross-tabulating with a separate **Ethnicity** field if available.  
- For compliance with current standards (e.g., *OMB Directive 15*), prefer maintaining distinct **Race** and **Ethnicity** fields.

#### 📚 Source
Adapted from law-enforcement, public-health, and census-derived race code sets.
{% enddocs %}
